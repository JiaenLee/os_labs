#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <semaphore.h>
#include <time.h>

#define MSG_PASSING 1
#define SHARED_MEM 2

#define MAX_MSG_SIZE 1024

#define NORMAL_MSG 1
#define EXIT_MSG 2


typedef struct {
    int flag;               // 1 for message passing, 2 for shared memory 紀錄使用那一種通訊方式
    sem_t *sender_sem;      // POSIX semaphore：通知 Receiver，Sender 已經寫入一則訊息
    sem_t *receiver_sem;    // POSIX semaphore：通知 Sender，Receiver 已經讀取完畢，可以寫入下一則訊息
    //union 的意思是同一段記憶體依照使用情境存放不同的資料。當 flag == 1 時使用 msqid；當 flag == 2 時使用 shm_addr。
    union{
        int msqid;          //Message Queue 的識別碼
        char* shm_addr;     //使用shared memory 時存放共享記憶體的位置
    }storage;
} mailbox_t;


typedef struct {
    long mType; //訊息類型。1 代表一般訊息，2 代表結束訊息
    char msgText[MAX_MSG_SIZE]; //真正的文字內容
} message_t;

double receive(message_t* message_ptr, mailbox_t* mailbox_ptr);
double send(message_t message, mailbox_t* mailbox_ptr);
