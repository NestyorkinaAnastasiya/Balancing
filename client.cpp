#include "mpi.h"
#include "task.cpp"
#include <iostream>
#include <fstream>
#define MAX_DATA 1000
#define ERROR_INIT_BARRIER -14
MPI_Comm currentComm = MPI_COMM_WORLD;
MPI_Comm newComm;
int ids[6] = {0,1,2,3,4,5};
//четрыре объекта типа "описатель потока"
pthread_t thrs[6];
pthread_barrier_t barrier;
int rank, size;
int condition = 0;
bool changeComm = false;
int countOfWorkers = 1;
int countOfThreads = 2;


//функция потока
void* worker(void* me)
{
        int  exitTask = 0;

       // Запрашиваем по одной задаче от каждого узла кроме самого себя
        for (int i = 0 ; i < size; i++)
        {
                if(changeComm)
                {
                        MPI_Barrier(currentComm);
                        pthread_barrier_wait(&barrier);
                        condition = 1;
                        MPI_Send(&condition, 1, MPI_INT, rank, 1, currentComm);
                        while (changeComm);
                }
                if (i != rank)
                {       // Отправляем запрос на получение задачи (состояние здесь пока не причём
                        // в разработке)
                        condition = 0;
                        MPI_Send(&condition, 1, MPI_INT, i, 1, currentComm);
                        MPI_Status st;
                        // Получаем результат запроса в виде true или false
                        MPI_Recv(&exitTask, 1, MPI_INT, i, 2, currentComm, &st);

                        // Если узел готов отправить задачу на обработку (до этого у меня пока не
                        // доходило)
                        if (exitTask)
                        {
                                // Получаем данные задачи
                                int begin, end;
                                MPI_Recv(&begin, 1, MPI_INT, i, 3, currentComm, &st);
                                MPI_Recv(&end, 1, MPI_INT, i, 3, currentComm, &st);
                                Task t(begin, end);
                               //delete[] buf;
                              
                                pthread_mutex_lock(&mutex);
                                std::cerr << "task run:: rank " << rank << " from rank " << i << "\n";
                                pthread_mutex_unlock(&mutex);

                                // Если задача от текущего узла была получена, берём задачи из
                                // этого узла до тех пор, пока задачи на нём не будут исчерпаны
                                i--;
                        }
                }
        }
        // -//- процесс смог успешно завершиться
        pthread_mutex_lock(&mutex);
        std::cerr << "rank " << rank << ":: FINISHED THREAD\n";
        pthread_mutex_unlock(&mutex);

}

void* dispatcher(void* me)
{
        Task *t;
        int cond;
        int countOfAsks = 0;
        // Пока не получены сообщения от макс кол-ва узлов (после завершения собственных задач процесса)
        while(countOfAsks < (size-1)*countOfWorkers)
        {
                MPI_Status st;
                // Получаем запрос от любого узла
                MPI_Recv(&cond, 1, MPI_INT, MPI_ANY_SOURCE, 1, currentComm, &st);

                if (!cond)
                {
                        // Получаем номер этого узла
                        int peer = st.MPI_SOURCE;
                        int send = 0;
                        // Отправляем сообщение о том, что задач нет
                        MPI_Send(&send, 1, MPI_INT, peer, 2, currentComm);
                        countOfAsks++;
                } 
                 else
                {      
                        for (int j = 0; j < countOfThreads - 1; j++) 
                                MPI_Recv(&cond, 1, MPI_INT, rank, 1, currentComm, &st);
                        
                        currentComm = newComm;
                        MPI_Comm_rank(currentComm, &rank);
                        MPI_Comm_size(currentComm, &size);
                        changeComm = false;
                }
        }
        // Сообщение об окончании работы диспетчера (отладка)
        std::cerr << "rank " << rank << ":: despatcher closed.\n";
}
     
void* server(void *me)
{
        MPI_Comm client;
        MPI_Status status;
        char port_name[MPI_MAX_PORT_NAME];
        double buf[MAX_DATA];
        int again;
        MPI_Open_port(MPI_INFO_NULL, port_name);
                
        MPI_Comm_accept(port_name, MPI_INFO_NULL, 0, currentComm, &client);
        again = 1;

        MPI_Intercomm_merge(client, false, &newComm);
        changeComm = true;
        //while (changeComm);
}

int main( int argc, char **argv )
{
	MPI_Comm server;
	double buf[MAX_DATA];
	bool done = false;
	char port_name[MPI_MAX_PORT_NAME];
	std::ifstream fPort("port_name.txt");
	for(int i = 0; i < MPI_MAX_PORT_NAME; i++)
		fPort >> port_name[i]; 
	fPort.close();
	MPI_Init(&argc, &argv);
	/* assume server’s name is cmd-line arg */
	MPI_Comm_connect(port_name, MPI_INFO_NULL, 0, currentComm, &server);
	MPI_Intercomm_merge(server, true, &newComm); 
	currentComm = newComm;
        MPI_Comm_rank(currentComm, &rank);
        MPI_Comm_size(currentComm, &size);
       
        int status = pthread_barrier_init(&barrier, NULL,countOfWorkers);
        if(status!=0)
        {
                printf("main error: can't init barrier, status = %d\n", status);
                exit(ERROR_INIT_BARRIER);       
        }

        //атрибуты потока
        pthread_attr_t attrs;

        //инициализация атрибутов потока
        if(0!=pthread_attr_init(&attrs))
        {
                perror("Cannot initialize attributes");
                abort();
        };
        //установка атрибута "присоединенный"
        if(0!=pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE))
        {
                perror("Error in setting attributes");
                abort();
        }
        //порождение четырех потоков
        for(int i = 0; i < countOfWorkers; i++)
        if(0!=pthread_create(&thrs[i], &attrs, worker, &ids[i]))
        {
                perror("Cannot create a thread");
                abort();
        }

        if(0!=pthread_create(&thrs[1], &attrs, dispatcher, &ids[1]))
        {
                perror("Cannot create a thread");
                abort();
        }

//        if(0!=pthread_create(&thrs[5], &attrs, server, &ids[5]))
//        {
//                perror("Cannot create a thread");
//               abort();
//        }

    //освобождение ресурсов, занимаемых описателем атрибутов
        pthread_attr_destroy(&attrs);
    //ожидание завершения порожденных потоков
        for(int i = 0; i < countOfThreads; i++)
                if(0!=pthread_join(thrs[i], NULL))
                {
                        perror("Cannot join a thread");
                        abort();
                }
        std:: cerr << "threads finished\n";
	MPI_Reduce(&globalRes,&resultOfGlobalTask,1, MPI_INT, MPI_SUM, 0, currentComm);
	MPI_Finalize();
	return 0;
}
