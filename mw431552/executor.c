#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>

#include "utils.h"
#include "err.h"

#define LINE_SIZE 1024
#define LINE_SIZE_INPUT 512
#define COMMAND_SIZE 30

#define RUN 'r'
#define OUT 'o'
#define ERR 'e'
#define KILL 'k'
#define SLEEP 's'
#define QUIT 'q'
#define EMPTY_LINE '\0'
#define MAX_N_TASKS 4097
#define MIKRO_TO_MILI 1000

#define EMPTY_EXITCODE 10000
#define EMPTY_THREAD 5000

struct Task {
    int id;
    int pipe_dsc[2];
    int pipe_dsc_err[2];

    char message_out[LINE_SIZE];
    char message_err[LINE_SIZE];

    pthread_t thread;
    pid_t expected_child;

    int is_active;
};

struct Queue {
    int thread_id[MAX_N_TASKS];
    int status[MAX_N_TASKS];
    int begin;
    int end;
    int q_size;
    pthread_mutex_t queue_mutex;
};

struct Task tasks[MAX_N_TASKS];
pthread_mutex_t mutex_tasks[MAX_N_TASKS];
pthread_t thread_out[MAX_N_TASKS];
pthread_t thread_err[MAX_N_TASKS];

sem_t MUTEX;
pthread_mutex_t  MUTEX_THREAD;
pthread_mutex_t MUTEX_CHECK;
sem_t  SEMAPHORE_WRITER;
sem_t  SEMAPHORE_EXECUTOR;
pthread_t writer_thread;

int number_of_threads_willing_to_die;
int was_executor_post;

bool command;

struct Queue queue_write;
struct Queue queue_write_no_command;

void init_queue(struct Queue *q) {
    pthread_mutex_init(&q->queue_mutex, NULL);

    for (int i = 0; i < MAX_N_TASKS; i++) {
        q->status[i] = EMPTY_EXITCODE;
        q->thread_id[i] = EMPTY_THREAD;
    }

    q->q_size = MAX_N_TASKS;
    q->begin = 0;
    q->end = 0;
}

int is_empty_queue(struct Queue *q) {
    if (q->begin == q->end) {
        return 1;
    }
    else {
        return 0;
    }
}

void push(struct Queue *q, int thread_id, int status) {
    pthread_mutex_lock(&(q->queue_mutex));
    q->thread_id[q->end] = thread_id;
    q->status[q->end] = status;
    q->end = (q->end + 1) % (q->q_size);
    pthread_mutex_unlock(&(q->queue_mutex));
}

void pop(struct Queue *q, int *thread_id, int *status) {
    *thread_id = q->thread_id[q->begin];
    *status = q->status[q->begin];
    q->begin = (q->begin + 1) % (q->q_size);
}

void destroy_mutex_queue(struct Queue *q) {
    pthread_mutex_destroy(&q->queue_mutex);
}

void write_run(int id, pid_t pid) {
    printf("Task %d started: pid %d.\n", id, pid);
}

void write_out(int id, char *s) {
    printf("Task %d stdout: '%s'.\n", id, s);
}

void write_err(int id, char *s) {
    printf("Task %d stderr: '%s'.\n", id, s);
}

void write_fin_signalled(int id) {
    printf("Task %d ended: signalled.\n", id);
}

void write_fin_standard(int id, int exit_code) {
    if (exit_code != EMPTY_EXITCODE) {
        printf("Task %d ended: status %d.\n", id, exit_code);
    }
    else {
        write_fin_signalled(id);
    }
}

void command_true() {
    sem_wait(&MUTEX);
    command = true;
    sem_post(&MUTEX);
}

void command_false() {
    sem_wait(&MUTEX);
    command = false;
    sem_post(&MUTEX);
}

void get_err(int id) {
    pthread_mutex_lock(&(mutex_tasks[id]));  

    char *message = tasks[id].message_err;
    write_err(id, message);

    pthread_mutex_unlock(&(mutex_tasks[id]));
}

void get_out(int id) {
    pthread_mutex_lock(&(mutex_tasks[id]));

    char *message = tasks[id].message_out;
    write_out(id, message);

    pthread_mutex_unlock(&(mutex_tasks[id]));
}

void kill_other(int id) {
    kill(tasks[id].expected_child, SIGINT);
}

void destroy_everything(char **run_table) {
    if (run_table != NULL) {
        free_split_string(run_table);
    }

    sem_destroy(&MUTEX);
    pthread_mutex_destroy(&MUTEX_THREAD);
    pthread_mutex_destroy(&MUTEX_CHECK);

    sem_destroy(&SEMAPHORE_WRITER);
    sem_destroy(&SEMAPHORE_EXECUTOR);

    destroy_mutex_queue(&queue_write);
    destroy_mutex_queue(&queue_write_no_command);
}

void get_from_queue() {
    sem_wait(&MUTEX);
    pthread_mutex_lock(&(queue_write.queue_mutex));

    while (is_empty_queue(&queue_write) == 0) {
        int id;
        int status;
        pop(&queue_write, &id, &status);
        pthread_join(tasks[id].thread, NULL);
        tasks[id].is_active = 0;
        __atomic_fetch_add(&number_of_threads_willing_to_die, -1, __ATOMIC_SEQ_CST);
        write_fin_standard(id, status);
    }

    pthread_mutex_unlock(&(queue_write.queue_mutex));
    sem_post(&MUTEX);
}


void get_from_queue_without_join() {
    sem_wait(&MUTEX);
    pthread_mutex_lock(&(queue_write.queue_mutex));

    while (is_empty_queue(&queue_write) == 0) {
        int id;
        int status;
        pop(&queue_write, &id, &status);
        write_fin_standard(id, status);
    }

    pthread_mutex_unlock(&(queue_write.queue_mutex));
    sem_post(&MUTEX);
}

void end_of_everything(char **run_table) {
    pthread_mutex_lock(&MUTEX_CHECK);
    was_executor_post = 2;
    pthread_mutex_unlock(&MUTEX_CHECK);

    // Przekazanie sekcji krytycznej
    // watki ktore sie beda konczyc teraz beda wrzucane na kolejke.
    sem_post(&SEMAPHORE_WRITER);
    pthread_join(writer_thread, NULL);

    for (int i = 0; i < MAX_N_TASKS; i++) {
        if (tasks[i].is_active == 1) {
            kill(tasks[i].expected_child, SIGKILL);
            pthread_join(tasks[i].thread, NULL);
            pthread_mutex_destroy(&(mutex_tasks[i]));
        }
    }

    get_from_queue_without_join();
    destroy_everything(run_table);
    exit(0);
}

void *reading_pipe_err(void *info) {
    char buffer_err[LINE_SIZE];

    struct Task *current_task = info;
    int id = current_task->id;
    FILE *file_err = fdopen(tasks[id].pipe_dsc_err[0], "r");

    while(read_line(buffer_err, LINE_SIZE, file_err) ) {
        pthread_mutex_lock(&(mutex_tasks[id]));

        strcpy(tasks[id].message_err, buffer_err);
        int size = strlen(tasks[id].message_err)-1;
        tasks[id].message_err[size] = EMPTY_LINE;

        pthread_mutex_unlock(&(mutex_tasks[id]));
    }

    return 0;
}

void *reading_pipe(void *info) {
    char buffer[LINE_SIZE];
    struct Task *current_task = info;
    int id = current_task->id;

    FILE *file = fdopen(tasks[id].pipe_dsc[0], "r");

    while(read_line(buffer, LINE_SIZE, file)) {
        pthread_mutex_lock(&(mutex_tasks[id]));

        strcpy(tasks[id].message_out, buffer);
        int size = strlen(tasks[id].message_out)-1;
        tasks[id].message_out[size] = EMPTY_LINE;

        pthread_mutex_unlock(&(mutex_tasks[id]));
    }

    return 0;
}

void *wrapper_thread(void *info) {
    struct Task *current_task = info;
    int id = current_task->id;

    pthread_mutex_lock(&(mutex_tasks[id]));

    pthread_mutex_lock(&MUTEX_THREAD);
    pthread_create(&(thread_out[id]), NULL, reading_pipe, &(tasks[id]));
    pthread_create(&(thread_err[id]), NULL, reading_pipe_err, &(tasks[id]));
    pthread_mutex_unlock(&MUTEX_THREAD);

    pthread_mutex_unlock(&(mutex_tasks[id]));

    pthread_join(thread_err[id], NULL);
    pthread_join(thread_out[id], NULL);
    int status;
    __atomic_fetch_add(&number_of_threads_willing_to_die, 1, __ATOMIC_SEQ_CST);
    ASSERT_SYS_OK(waitpid(tasks[id].expected_child, &status, 0));

    if (WIFEXITED(status)) {
        int child_status = WEXITSTATUS(status);
        sem_wait(&MUTEX);
        int is_command = command;
        if (is_command == 0) {
            push(&queue_write_no_command, id, child_status);
            sem_post(&SEMAPHORE_WRITER);
           // PRZENIESIENIE SEKCJI KRYTYCZNEJ -> konczymy w wypisywarce, dlateo tu nie robimy unlock
        }
        else {
            push(&queue_write, id, child_status);
            sem_post(&MUTEX);
        }
    } 
    else {
        sem_wait(&MUTEX);

        int is_command = command;
        if (is_command == 0) {
            push(&queue_write_no_command, id, EMPTY_EXITCODE);
            sem_post(&SEMAPHORE_WRITER);               
        }
        else {
            push(&queue_write, id, EMPTY_EXITCODE);
            sem_post(&MUTEX);
        }
    }

    return 0;
}

void execute_run(char **run_table, int id) {
    ASSERT_SYS_OK(pipe(tasks[id].pipe_dsc));
    ASSERT_SYS_OK(pipe(tasks[id].pipe_dsc_err));
    set_close_on_exec(tasks[id].pipe_dsc[0], 1);
    set_close_on_exec(tasks[id].pipe_dsc_err[0], 1);
    set_close_on_exec(tasks[id].pipe_dsc[1], 1);
    set_close_on_exec(tasks[id].pipe_dsc_err[1], 1);

    pid_t pid1;
    pid1 = fork();
    ASSERT_SYS_OK(pid1);
    char **run_copy = run_table+1;
    if (!pid1) {
        dup2(tasks[id].pipe_dsc[1], STDOUT_FILENO);
        dup2(tasks[id].pipe_dsc_err[1], STDERR_FILENO);
        set_close_on_exec(tasks[id].pipe_dsc[1], 1);
        set_close_on_exec(tasks[id].pipe_dsc[0], 1);
        set_close_on_exec(tasks[id].pipe_dsc_err[1], 1);
        set_close_on_exec(tasks[id].pipe_dsc_err[0], 1);

        ASSERT_SYS_OK(close(tasks[id].pipe_dsc[0]));
        ASSERT_SYS_OK(close(tasks[id].pipe_dsc_err[0]));
        ASSERT_SYS_OK(close(tasks[id].pipe_dsc[1]));
        ASSERT_SYS_OK(close(tasks[id].pipe_dsc_err[1]));

        ASSERT_SYS_OK(execvp(run_copy[0], run_copy));
    }
    else {
        write_run(id, pid1);

        ASSERT_SYS_OK(close(tasks[id].pipe_dsc[1]));
        ASSERT_SYS_OK(close(tasks[id].pipe_dsc_err[1]));

        pthread_mutex_init(&(mutex_tasks[id]), NULL);

        pthread_mutex_lock(&(mutex_tasks[id]));
        pthread_mutex_lock(&MUTEX_THREAD);
        tasks[id].expected_child = pid1;
        tasks[id].id = id;
        tasks[id].is_active = 1;
        pthread_create(&(tasks[id].thread), NULL, wrapper_thread, &(tasks[id]));
        pthread_mutex_unlock(&MUTEX_THREAD);
        pthread_mutex_unlock(&(mutex_tasks[id]));
    }
}

void *writer () {
    while (true) {
        sem_wait(&SEMAPHORE_WRITER);
        pthread_mutex_lock(&MUTEX_CHECK);
        if (was_executor_post == 2) {        
            pthread_mutex_unlock(&MUTEX_CHECK);

            // W przypadku kiedy zakanczamy prace, musimy wypisac wszystkie zakonczone do tej pory 
            // (wczesniej) programy.
            while (!is_empty_queue(&queue_write_no_command)){
                int id;
                int status;
                pthread_mutex_lock(&(queue_write_no_command.queue_mutex));
                pop(&queue_write_no_command, &id, &status);
                pthread_mutex_unlock(&(queue_write_no_command.queue_mutex));
                pthread_join(tasks[id].thread, NULL);
                tasks[id].is_active = 0;
                __atomic_fetch_add(&number_of_threads_willing_to_die, -1, __ATOMIC_SEQ_CST);
                write_fin_standard(id, status);
            }
            break;
        }
        else {
            pthread_mutex_unlock(&MUTEX_CHECK);
        }
        int id;
        int status;
        pthread_mutex_lock(&(queue_write_no_command.queue_mutex));
        pop(&queue_write_no_command, &id, &status);
        pthread_mutex_unlock(&(queue_write_no_command.queue_mutex));

        pthread_join(tasks[id].thread, NULL);
        tasks[id].is_active = 0;
        __atomic_fetch_add(&number_of_threads_willing_to_die, -1, __ATOMIC_SEQ_CST);
        write_fin_standard(id, status);

        // Aparat pilnujacy, by wszystkie rozpoczate watki sie wypisywaly od razu i dopiero po
        // jego zakonczeniu byla mozliwosc pojscia dalej przez egzekutora, czyli
        // najpierw wypisujemy cala nasza zawartosc, a dopiero potem pozwalamy egzekutorowi pojsc naprzod.
        pthread_mutex_lock(&MUTEX_CHECK);
        if ((number_of_threads_willing_to_die == 0) && (was_executor_post == 0)) {
            was_executor_post = 1;
            sem_post(&SEMAPHORE_EXECUTOR);
            // Dzieki temu juz nikt nie wejdzie w miedzyczasie.
            command = true;
            sem_post(&MUTEX);
            pthread_mutex_unlock(&MUTEX_CHECK);
        }
        else {
            sem_post(&MUTEX);
            pthread_mutex_unlock(&MUTEX_CHECK);
        }
    }

    return 0;
}

void read_input() {
    char str[LINE_SIZE_INPUT];
    int id = 0;
    // Wypisywarka - za jej pomoca mozemy miec zapewnione, ze podczas wypisania ostatniego
    // zakonczenia, beda tylko maksymalnie 2 otwarte watki - egzekutor i wlasnie wypisywarka.
    pthread_create(&writer_thread, NULL, writer, NULL);

    while (read_line(str, LINE_SIZE_INPUT, stdin)) {
        // Czytamy tablice, w tym najwazniejsza informacje - pierwsza litere pierwszego slowa,
        // ktora determinuje nam polecenie.
        char **run_table = split_string(str);
        char id_command = run_table[0][0];

        // Polecenie moze byc obsluzone dopiero, gdy zostana wypisane wszystkie zakonczone do tej
        // pory watki.
        pthread_mutex_lock(&MUTEX_CHECK);
        was_executor_post = 0;
        // Rozpoczynamy obsluge polecenia, ustawiamy zmienna odpowiadajaca za informacje na 1 (true)/
        if (number_of_threads_willing_to_die != 0) {
            pthread_mutex_unlock(&MUTEX_CHECK);
            sem_wait(&SEMAPHORE_EXECUTOR);
        }            
        else {
            pthread_mutex_unlock(&MUTEX_CHECK);
            command_true();
        }

        if ((id_command != EMPTY_LINE)) {
            if (id_command == QUIT) {
                end_of_everything(run_table);
            }
            else if (id_command == RUN) {
                execute_run(run_table, id);
                id++;
            }
            else {
                int number = atoi(run_table[1]);
                if (id_command == OUT) {
                    get_out(number);
                }
                else if (id_command == ERR) {
                    get_err(number);
                }
                else if (id_command == KILL) {
                    kill_other(number);
                }
                else if (id_command == SLEEP) {
                    usleep(MIKRO_TO_MILI * number);
                }
            }
        }
        command_false();

        free_split_string(run_table);
        get_from_queue();
    }

    char **nothing = NULL;
    // Obsluge zakonczenia traktuje jak obsluge polecenia, nie
    // jest to zdefiniowane dokladnie w tresci zadania, zakladam
    // tak ze wzgledu na wygode implementacji.
    command_true();
    end_of_everything(nothing);
}

void set_default() {
    // Semafor pilnujacy, by wypisaly sie wszystkie watki konczace sie w przerwie miedzy
    // poleceniami i dopiero potem zadzialal egzekutor.
    sem_init(&SEMAPHORE_EXECUTOR, 0, 0);
    // Mutex zapewniajacy, ze sprawdzenie wartosci <number_of_threads_willing_to_die> 
    // bedzie wykonywane tylko przez 1 watek.
    pthread_mutex_init(&MUTEX_CHECK, NULL);
    number_of_threads_willing_to_die = 0;
    was_executor_post = 0;
    sem_init(&MUTEX, 0, 1);
    pthread_mutex_init(&MUTEX_THREAD, NULL);
    sem_init(&SEMAPHORE_WRITER, 0, 0);

    command = false;

    // Na poczatku nie ma zadnych aktywnych taskow.
    for (int i = 0; i < MAX_N_TASKS; i++) {
        tasks[i].is_active = 0;
    }
}

int main() {
    set_default();
    
    init_queue(&queue_write);
    init_queue(&queue_write_no_command);

    read_input();
}