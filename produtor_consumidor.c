/* File:
 *    produtor_consumidor.c
 *
 * Purpose:
 *    Problema do Produtor/Consumidor com relógios vetoriais.
 *    3 threads produtoras geram relógios vetoriais e os colocam em uma fila
 *    limitada; 3 threads consumidoras retiram os relógios da fila e os
 *    imprimem na saída padrão. A sincronização usa 1 mutex e 2 variáveis de
 *    condição (condFull / condEmpty), como no exemplo pth_pool.c.
 *
 * Compile:  gcc -g -Wall -o produtor_consumidor produtor_consumidor.c -lpthread
 * Usage:    ./produtor_consumidor <cenario>
 *              cenario 1: fila CHEIA  (produtor 1 relógio/s, consumidor 1 relógio/2s)
 *              cenario 2: fila VAZIA  (produtor 1 relógio/2s, consumidor 1 relógio/s)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define NUM_PRODUCERS 3   // Threads produtoras
#define NUM_CONSUMERS 3   // Threads consumidoras
#define CLOCK_SIZE    3   // Componentes do relógio vetorial (1 por produtor)
#define BUFFER_SIZE   4   // Tamanho da fila (pequeno para evidenciar fila cheia)
#define CLOCKS_PER_PRODUCER 6 // Relógios que cada produtor gera antes de terminar

typedef struct Clock {
   int p[CLOCK_SIZE];
} Clock;

/* Fila circular limitada */
Clock queue[BUFFER_SIZE];
int in = 0, out = 0, count = 0;
int done = 0;             // 1 quando todos os produtores terminaram

pthread_mutex_t mutex;
pthread_cond_t condFull;  // produtores esperam aqui quando a fila está cheia
pthread_cond_t condEmpty; // consumidores esperam aqui quando a fila está vazia

int producerDelay;        // segundos entre duas produções
int consumerDelay;        // segundos entre dois consumos

void printClock(const char* who, long id, const Clock* c) {
   printf("(%s %ld) Relogio: [", who, id);
   int i;
   for (i = 0; i < CLOCK_SIZE; i++)
      printf("%d%s", c->p[i], i < CLOCK_SIZE - 1 ? ", " : "");
   printf("]\n");
}

/* Coloca um relógio na fila; bloqueia enquanto ela estiver cheia */
void putClock(Clock c, long id) {
   pthread_mutex_lock(&mutex);

   int warned = 0;
   while (count == BUFFER_SIZE) {
      if (!warned) {
         printf("   >> FILA CHEIA: produtor %ld bloqueado\n", id);
         warned = 1;
      }
      pthread_cond_wait(&condFull, &mutex);
   }

   queue[in] = c;
   in = (in + 1) % BUFFER_SIZE;
   count++;
   printf("(Produtor %ld) colocou relogio na fila (%d/%d)\n", id, count, BUFFER_SIZE);

   pthread_mutex_unlock(&mutex);
   pthread_cond_signal(&condEmpty);
}

/* Retira um relógio da fila; bloqueia enquanto ela estiver vazia.
 * Retorna 0 se não há mais relógios e os produtores já terminaram. */
int getClock(Clock* c, long id) {
   pthread_mutex_lock(&mutex);

   int warned = 0;
   while (count == 0) {
      if (done) {
         pthread_mutex_unlock(&mutex);
         return 0;
      }
      if (!warned) {
         printf("   >> FILA VAZIA: consumidor %ld bloqueado\n", id);
         warned = 1;
      }
      pthread_cond_wait(&condEmpty, &mutex);
   }

   *c = queue[out];
   out = (out + 1) % BUFFER_SIZE;
   count--;

   pthread_mutex_unlock(&mutex);
   pthread_cond_signal(&condFull);
   return 1;
}

void* producer(void* args) {
   long id = (long) args;
   Clock local;
   memset(&local, 0, sizeof(local));

   int i;
   for (i = 0; i < CLOCKS_PER_PRODUCER; i++) {
      sleep(producerDelay);
      local.p[id]++;            // evento local: incrementa o próprio componente
      putClock(local, id);      // coloca uma cópia do relógio na fila
   }
   return NULL;
}

void* consumer(void* args) {
   long id = (long) args;
   Clock c;

   while (getClock(&c, id)) {
      printClock("Consumidor", id, &c);
      sleep(consumerDelay);
   }
   return NULL;
}

/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
   if (argc != 2 || (atoi(argv[1]) != 1 && atoi(argv[1]) != 2)) {
      fprintf(stderr, "Uso: %s <cenario>\n", argv[0]);
      fprintf(stderr, "  1: fila cheia  (produtor 1s, consumidor 2s)\n");
      fprintf(stderr, "  2: fila vazia  (produtor 2s, consumidor 1s)\n");
      return 1;
   }

   if (atoi(argv[1]) == 1) {
      producerDelay = 1; consumerDelay = 2;
      printf("=== Cenario 1: fila CHEIA (produz 1/s, consome 1/2s) ===\n");
   } else {
      producerDelay = 2; consumerDelay = 1;
      printf("=== Cenario 2: fila VAZIA (produz 1/2s, consome 1/s) ===\n");
   }

   pthread_mutex_init(&mutex, NULL);
   pthread_cond_init(&condFull, NULL);
   pthread_cond_init(&condEmpty, NULL);

   pthread_t prod[NUM_PRODUCERS], cons[NUM_CONSUMERS];
   long i;
   for (i = 0; i < NUM_PRODUCERS; i++)
      if (pthread_create(&prod[i], NULL, &producer, (void*) i) != 0)
         perror("Failed to create producer");
   for (i = 0; i < NUM_CONSUMERS; i++)
      if (pthread_create(&cons[i], NULL, &consumer, (void*) i) != 0)
         perror("Failed to create consumer");

   for (i = 0; i < NUM_PRODUCERS; i++)
      if (pthread_join(prod[i], NULL) != 0)
         perror("Failed to join producer");

   /* Todos os produtores terminaram: avisa os consumidores */
   pthread_mutex_lock(&mutex);
   done = 1;
   pthread_cond_broadcast(&condEmpty);
   pthread_mutex_unlock(&mutex);

   for (i = 0; i < NUM_CONSUMERS; i++)
      if (pthread_join(cons[i], NULL) != 0)
         perror("Failed to join consumer");

   pthread_mutex_destroy(&mutex);
   pthread_cond_destroy(&condFull);
   pthread_cond_destroy(&condEmpty);
   printf("=== Fim ===\n");
   return 0;
}  /* main */
