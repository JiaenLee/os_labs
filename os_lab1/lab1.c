#include "lab1.h"

static double elapsed_seconds(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) * 1e-9;
}

double receive(message_t *message_ptr, mailbox_t *mailbox_ptr) {
    struct timespec start, end;
    //----------------message passing-------------------
    if (mailbox_ptr->flag == MSG_PASSING) {

        //for (;;) 不斷重試，直到成功接收訊息，或遇到無法處理的錯誤。
        for (;;) {  
            // 記錄開始時間
            if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }
            //嘗試接收訊息, IPC_NOWAIT 表示如果目前沒有訊息就立即回傳，而不是阻塞等待。
            int result = msgrcv(mailbox_ptr->storage.msqid, message_ptr, sizeof(message_ptr->msgText), 0, IPC_NOWAIT);
            int error = errno;

            // 記錄結束時間
            if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }

            if (result != -1) { //若接收成功，回傳這次呼叫所花的時間
                return elapsed_seconds(start, end);
            }

            ////若錯誤是 ENOMSG（目前沒有訊息）或 EINTR（呼叫被訊號中斷），就用 nanosleep 短暫休息後重試。這段休息時間不會累計
            if (error != ENOMSG && error != EINTR) {    
                errno = error;
                perror("msgrcv");
                exit(EXIT_FAILURE);
            }
            struct timespec delay = {0, 1000000};// 設定休息時間為 0 秒、1 毫秒。
            // 呼叫 nanosleep 休息。如果休息被訊號中斷，nanosleep 會回傳 -1，並把尚未休完的時間寫回 delay，迴圈就用剩餘時間繼續休息
            while (nanosleep(&delay, &delay) == -1) {
                // 如果錯誤不是 EINTR（訊號中斷），就印出錯誤訊息並結束程式
                if (errno != EINTR) {
                    perror("nanosleep");
                    exit(EXIT_FAILURE);
                }
            }
        }
    } 
    //----------------share memory-------------------    
    else if (mailbox_ptr->flag == SHARED_MEM) {
        // 等待 sender 寫入一則完整訊息後再讀取。
        if (sem_wait(mailbox_ptr->sender_sem) == -1) {
            perror("sem_wait sender");
            exit(EXIT_FAILURE);
        }
        if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
            perror("clock_gettime");
            exit(EXIT_FAILURE);
        }
        // 從共享記憶體複製訊息到 message_ptr
        memcpy(message_ptr, mailbox_ptr->storage.shm_addr, sizeof(*message_ptr));

        if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
            perror("clock_gettime");
            exit(EXIT_FAILURE);
        }

        // 通知 sender 已讀取完畢，可以寫入下一則訊息。
        if (sem_post(mailbox_ptr->receiver_sem) == -1) {
            perror("sem_post receiver");
            exit(EXIT_FAILURE);
        }

        return elapsed_seconds(start, end);
    }
    return 0.0;
}

double send(message_t message, mailbox_t *mailbox_ptr) {
    struct timespec start, end;
    //----------------message passing-------------------
    if (mailbox_ptr->flag == MSG_PASSING) {
        for (;;) {
            if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }
            //嘗試傳送訊息, IPC_NOWAIT 表示如果佇列已滿就立即回傳，而不是阻塞等待。
            int result = msgsnd(mailbox_ptr->storage.msqid, &message, sizeof(message.msgText), IPC_NOWAIT);
            int error = errno;

            if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }

            if (result != -1) {
                return elapsed_seconds(start, end);
            }
            if (error != EAGAIN && error != EINTR) {
                errno = error;
                perror("msgsnd");
                exit(EXIT_FAILURE);
            }            
            struct timespec delay = {0, 1000000};
            while (nanosleep(&delay, &delay) == -1) {
                if (errno != EINTR) {
                    perror("nanosleep");
                    exit(EXIT_FAILURE);
                }
            }

        }
    } 
    //----------------share memory-------------------  
    else if (mailbox_ptr->flag == SHARED_MEM) {
        // 等待 receiver 讀完上一則訊息，避免覆寫尚未讀取的資料。
        if (sem_wait(mailbox_ptr->receiver_sem) == -1) {
            perror("sem_wait receiver");
            exit(EXIT_FAILURE);
        }

        if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
            perror("clock_gettime");
            exit(EXIT_FAILURE);
        }

        // 將訊息複製到共享記憶體
        memcpy(mailbox_ptr->storage.shm_addr, &message, sizeof(message));
        if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
            perror("clock_gettime");
            exit(EXIT_FAILURE);
        }

        if (sem_post(mailbox_ptr->sender_sem) == -1) {
            perror("sem_post sender");
            exit(EXIT_FAILURE);
        }
        return elapsed_seconds(start, end);
    }
    return 0.0;
}

int main(int argc, char *argv[]) {
  /*
  main(argc, argv)
    │      │
    │      └── argv[]
    │             │
    │             ├── argv[0] = "./lab1"
    │             ├── argv[1] = "sender"
    │             ├── argv[2] = "1"
    │             └── argv[3] = "./message.txt"
    │
    └── argc = 4
    sender: ./lab1 sender 1 ./message.txt
    receiver:  ./lab1 receiver 1
  */
  
    //----------檢查參數數量----------
    if (argc < 3) {
        fprintf(stderr, "Usage: %s sender <mechanism> input_file \n", argv[0]);
        fprintf(stderr, "       %s receiver <mechanism> \n", argv[0]);
        return EXIT_FAILURE;
    }
  
    //----------判斷角色----------
    int is_sender;
    if (strcmp(argv[1], "sender") == 0) {
        is_sender = 1;
    } else if (strcmp(argv[1], "receiver") == 0) {
        is_sender = 0;
    } else {
        fprintf(stderr, "Role must be sender or receiver.\n");
        return EXIT_FAILURE;
    }
    

    //----------通訊方式----------
    int mechanism = atoi(argv[2]); //atoi() 將字串轉換成整數

    // 根據角色與通訊機制，選擇不同的暫存檔
    char output_file[64];

    if (strcmp(argv[1], "sender") == 0) {
        if (strcmp(argv[2], "1") == 0) {
            snprintf(output_file, sizeof(output_file), "sender_msg.txt");
        } else {
            snprintf(output_file, sizeof(output_file), "sender_shm.txt");
        }
    } else {
        if (strcmp(argv[2], "1") == 0) {
            snprintf(output_file, sizeof(output_file), "receiver_msg.txt");
        } else {
            snprintf(output_file, sizeof(output_file), "receiver_shm.txt");
        }
    }
    // 將 printf() 輸出導向對應的暫存檔
    if (freopen(output_file, "w", stdout) == NULL) {
        perror("freopen");
        return EXIT_FAILURE;
    }
    printf("Role: %s\n", argv[1]);
    fprintf(stderr, "Role: %s\n", argv[1]);
    
    //----------建立mailbox(初始設定, 還沒開始傳訊息)----------
    mailbox_t mailbox;
    mailbox.flag = mechanism;
    
    if (mechanism == MSG_PASSING) {

        printf("message passing\n");
        fprintf(stderr, "message passing\n");
        key_t key = 1234;   //佇列的識別鍵
        
        //利用 key 建立或取得訊息佇列，並將回傳的識別碼存進 mailbox.storage.msqid
        //IPC_CREAT：不存在時建立。0666：設定讀寫權限
        mailbox.storage.msqid = msgget(key, IPC_CREAT | 0666);

        if (mailbox.storage.msqid == -1) {
            perror("msgget");
            return EXIT_FAILURE;
        }

    }else if (mechanism == SHARED_MEM) {

        printf("shared memory\n");
        fprintf(stderr, "shared memory\n");
        key_t key = 5678;

        //建立或取得共享記憶體
        int shmid = shmget(key,sizeof(message_t),IPC_CREAT | 0666);

        if (shmid == -1) {
            perror("shmget");
            return EXIT_FAILURE;
        }
        //將共享記憶體連接到目前行程的位址空間
        mailbox.storage.shm_addr = shmat(shmid, NULL, 0);

        if (mailbox.storage.shm_addr == (void *) -1) {
            perror("shmat");
            return EXIT_FAILURE;
        }

        //開啟或建立具名 Semaphore

        // sender semaphore 初始為 0，receiver 需等 sender 傳送訊息。
        mailbox.sender_sem = sem_open("/os_lab1_sender", O_CREAT, 0666, 0);

        if (mailbox.sender_sem == SEM_FAILED) {
            perror("sem_open sender");
            return EXIT_FAILURE;
        }
        // receiver semaphore 初始為 1，讓 sender 一開始可以寫入第一則訊息。
        mailbox.receiver_sem = sem_open("/os_lab1_receiver", O_CREAT, 0666, 1);

        if (mailbox.receiver_sem == SEM_FAILED) {
            perror("sem_open receiver");
            return EXIT_FAILURE;
        }

    }    
    double communication_time = 0.0;

    //----------sender讀取檔案並呼叫send傳出----------
    
    if (is_sender) {
        //開檔案
        FILE *fp = fopen(argv[3], "r");

        if (fp == NULL) {
            perror("fopen");
            return EXIT_FAILURE;
        }
        //讀檔案並傳送( fgets() 讀取一行文字，直到遇到換行符號或檔案結尾  )
        message_t message;

        while (fgets(message.msgText, MAX_MSG_SIZE, fp) != NULL) {
            message.mType = NORMAL_MSG;
            fprintf(stderr, "Sent: %s", message.msgText);
            communication_time += send(message, &mailbox);
        }
        message_t exit_message = {0};
        exit_message.mType = EXIT_MSG;
        communication_time += send(exit_message, &mailbox);

        fclose(fp);
        printf("Sent: Total time in sending messages: %.6f s \n", communication_time);
        fprintf(stderr, "Sent: Total time in sending messages: %.6f s \n", communication_time);
    }
    //----------receiver呼叫receive接收訊息並印出----------
    else {
        message_t message;

        while (1) {
            communication_time += receive(&message, &mailbox);
            if (message.mType == EXIT_MSG) {
                break;
            }
            fprintf(stderr, "Received: %s", message.msgText);
        }

        // 刪除訊息佇列或共享記憶體，並移除具名 Semaphore 的名稱
        if (mechanism == MSG_PASSING) {

            // msgctl()控制訊息佇列的操作，IPC_RMID 表示刪除佇列)
            if (msgctl(mailbox.storage.msqid, IPC_RMID, NULL) == -1) {
                perror("msgctl");
                return EXIT_FAILURE;
            }

        } else if (mechanism == SHARED_MEM) {
            
            // 刪除共享記憶體
            int shmid = shmget(5678, sizeof(message_t), 0666);
            if (shmid >= 0 && shmctl(shmid, IPC_RMID, NULL) == -1) {
                perror("shmctl IPC_RMID");
                return EXIT_FAILURE;
            }
            sem_unlink("/os_lab1_sender");
            sem_unlink("/os_lab1_receiver");
        }
        printf("Received: Total time in receiving messages: %.6f s\n", communication_time);
        fprintf(stderr, "Received: Total time in receiving messages: %.6f s\n", communication_time);
    }

    

    if (mechanism == SHARED_MEM) {
        // 將共享記憶體從目前行程分離
        shmdt(mailbox.storage.shm_addr);
        //移除具名 Semaphore 的名稱；既有連接在關閉前仍可使用
        sem_close(mailbox.sender_sem);  
        sem_close(mailbox.receiver_sem);
    }   

    return EXIT_SUCCESS;
}
