#include "lab1.h"

static double elapsed_seconds(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) * 1e-9;
}

double receive(message_t *message_ptr, mailbox_t *mailbox_ptr) {
    struct timespec start, end;
    //----------------message passing-------------------
    if (mailbox_ptr->flag == MSG_PASSING) {
        for (;;) {  //for (;;) 不斷重試，直到成功接收訊息，或遇到無法處理的錯誤。
            if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }
            int result = msgrcv(mailbox_ptr->storage.msqid, message_ptr, sizeof(message_ptr->msgText), 0, IPC_NOWAIT);
            int error = errno;
            if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
                perror("clock_gettime");
                exit(EXIT_FAILURE);
            }
            if (result != -1) { //若接收成功，回傳這次呼叫所花的時間
                return elapsed_seconds(start, end);
            }
            if (error != ENOMSG && error != EINTR) {    //若錯誤是 ENOMSG（目前沒有訊息）或 EINTR（呼叫被訊號中斷），就用 nanosleep 短暫休息後重試。這段休息時間不會累計
                errno = error;
                perror("msgrcv");
                exit(EXIT_FAILURE);
            }
            // 設定休息時間為 0 秒、1,000,000 奈秒，也就是 1 毫秒。
            struct timespec delay = {0, 1000000};
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
        clock_gettime(CLOCK_MONOTONIC, &start);
        memcpy(message_ptr, mailbox_ptr->storage.shm_addr, sizeof(*message_ptr));
        clock_gettime(CLOCK_MONOTONIC, &end);
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
            int result = msgsnd(mailbox_ptr->storage.msqid, &message,
                                sizeof(message.msgText), IPC_NOWAIT);
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
        clock_gettime(CLOCK_MONOTONIC, &start);
        memcpy(mailbox_ptr->storage.shm_addr, &message, sizeof(message));
        clock_gettime(CLOCK_MONOTONIC, &end);
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
  
  //檢查參數數量
  if (argc < 3) {
    printf("Usage: %s sender <mechanism> input_file \n", argv[0]);
    printf("       %s receiver <mechanism> \n", argv[0]);
    return EXIT_FAILURE;
  }
  
  //判斷角色
    int is_sender;
    if (strcmp(argv[1], "sender") == 0) {
        is_sender = 1;
    } else if (strcmp(argv[1], "receiver") == 0) {
        is_sender = 0;
    } else {
        printf("Role must be sender or receiver.\n");
        return EXIT_FAILURE;
    }
    
    // 判斷通訊方式
    int mechanism = atoi(argv[2]); //atoi() 將字串轉換成整數
    
    //建立mailbox
    mailbox_t mailbox;
    mailbox.flag = mechanism;

    if (mechanism == MSG_PASSING) {
        key_t key = 1234;
        
        //利用 key 建立或取得訊息佇列，並將回傳的識別碼存進 mailbox.storage.msqid
        mailbox.storage.msqid = msgget(key, IPC_CREAT | 0666);

        if (mailbox.storage.msqid == -1) {
            perror("msgget");
            return EXIT_FAILURE;
        }
    }else if (mechanism == SHARED_MEM) {
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

    printf("Role: %s\n", argv[1]);
    printf("Mechanism: %d\n", mechanism);
    double communication_time = 0.0;

    if (is_sender) {
        printf("Input file: %s\n", argv[3]);
        //開檔案
        FILE *fp = fopen(argv[3], "r");

        if (fp == NULL) {
            perror("fopen");
            return EXIT_FAILURE;
        }
        //讀檔案並傳送
        message_t message;
        while (fgets(message.msgText, MAX_MSG_SIZE, fp) != NULL) {
            message.mType = NORMAL_MSG;

            communication_time += send(message, &mailbox);
        }

        message_t exit_message = {0};
        exit_message.mType = EXIT_MSG;
        communication_time += send(exit_message, &mailbox);

        fclose(fp);
    }else {
        message_t message;

        while (1) {
            communication_time += receive(&message, &mailbox);
            if (message.mType == EXIT_MSG) {
                break;
            }
            printf("%s", message.msgText);
        }
        // 刪除訊息佇列
        if (mechanism == MSG_PASSING) {
            if (msgctl(mailbox.storage.msqid, IPC_RMID, NULL) == -1) {
                perror("msgctl");
                return EXIT_FAILURE;
            }
        } else if (mechanism == SHARED_MEM) {
            // receiver 已回覆 exit message，sender 不再需要共享記憶體；在此清理 IPC 資源。
            shmdt(mailbox.storage.shm_addr);
            int shmid = shmget(5678, sizeof(message_t), 0666);
            if (shmid >= 0 && shmctl(shmid, IPC_RMID, NULL) == -1) {
                perror("shmctl IPC_RMID");
                return EXIT_FAILURE;
            }
            sem_close(mailbox.sender_sem);
            sem_close(mailbox.receiver_sem);
            sem_unlink("/os_lab1_sender");
            sem_unlink("/os_lab1_receiver");
        }
    }

    printf("Communication time: %.6f milliseconds\n", communication_time * 1000.0);

    if (mechanism == SHARED_MEM && is_sender) {
        shmdt(mailbox.storage.shm_addr);
        sem_close(mailbox.sender_sem);
        sem_close(mailbox.receiver_sem);
    }
    


    return EXIT_SUCCESS;
}
