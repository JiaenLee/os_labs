#include "lab1.h"

void receive(message_t *message_ptr, mailbox_t *mailbox_ptr) {
    //----------------message passing-------------------
    if (mailbox_ptr->flag == MSG_PASSING) {
        if (msgrcv(mailbox_ptr->storage.msqid,message_ptr,sizeof(message_ptr->msgText),0,0) == -1) {
            perror("msgrcv");
            exit(EXIT_FAILURE);
        }
    
    } 
    //----------------share memory-------------------    
    else if (mailbox_ptr->flag == SHARED_MEM) {
    
    
    }
}

void send(message_t message, mailbox_t *mailbox_ptr) {
    //----------------message passing-------------------
    if (mailbox_ptr->flag == MSG_PASSING) {
        //msgsnd(msqid, &message, message_size, 0); 將訊息放入 System V 訊息佇列。 0:一般阻塞式傳送設定
        if (msgsnd(mailbox_ptr->storage.msqid,&message,sizeof(message.msgText),0) == -1) {
            perror("msgsnd");
            exit(EXIT_FAILURE);
        }
    } 
    //----------------share memory-------------------  
    else if (mailbox_ptr->flag == SHARED_MEM) {
        //先指向共享記憶體所在位置
        message_t *shared_message =(message_t *)mailbox_ptr->storage.shm_addr;
        //把 message 的內容複製到共享記憶體。
        *shared_message = message;
        
    }
  
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
    }
    
    printf("Role: %s\n", argv[1]);
    printf("Mechanism: %d\n", mechanism);
    
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
            send(message, &mailbox);
        }
        
        message_t exit_message = {0};
        exit_message.mType = EXIT_MSG;
        send(exit_message, &mailbox);
        
        fclose(fp);
    }else {
        message_t message;

        while (1) {
            receive(&message, &mailbox);
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
        }
    }
    


    return EXIT_SUCCESS;
}
