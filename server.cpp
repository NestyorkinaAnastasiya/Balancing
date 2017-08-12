#include <mpi.h>
#include "task.cpp"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>
#define MAX_DATA 1000

#define ERROR_CREATE_THREAD   -11
#define ERROR_JOIN_THREAD     -12
#define BAD_MESSAGE           -13
#define ERROR_INIT_BARRIER    -14
#define ERROR_WAIT_BARRIER    -15
#define ERROR_DESTROY_BARRIER -16
#define SUCCESS                 0


int ids[6] = {0,1,2,3,4,5};
//четрыре объекта типа "описатель потока"
pthread_t thrs[3];
pthread_barrier_t barrier;
int rank, size;
int condition = 0;
MPI_Comm currentComm = MPI_COMM_WORLD;
MPI_Comm newComm;
bool changeComm = false;
bool serverFinished = false;
int countOfWorkers = 1;
int countOfThreads = 3;
std::queue<Task*> tasksContainer;

bool GetTask(Task **currTask)
{
	//{
        //std::lock_guard<std::mutex> mylock(mutex);
	pthread_mutex_lock(&mutex);
	if(tasksContainer.empty())
        {
		pthread_mutex_unlock(&mutex);
		return false;
	}
        else
        {
        	*currTask = tasksContainer.front();
		tasksContainer.pop();
        }
 	pthread_mutex_unlock(&mutex);
	//}
	return true;
}

void ChangeCommunicator()
{
        MPI_Barrier(currentComm);
        pthread_barrier_wait(&barrier);
        condition = 1;
        MPI_Send(&condition, 1, MPI_INT, rank, 1, currentComm);
        while(changeComm);
}

//функция потока
void* worker(void* me)
{
	Task *currTask;

	// Пока есть свои задачи - выполняем свои
	while (GetTask(&currTask))
        {
	        if(changeComm) ChangeCommunicator();
             	currTask -> Run();
        }
	int  exitTask = 0;
                
	// Запрашиваем по одной задаче от каждого узла кроме самого себя
	for (int i = 0 ; i < size || !serverFinished || changeComm; i++)
	{
		if(changeComm) ChangeCommunicator();

		if (i != rank && i < size)
		{	// Отправляем запрос на получение задачи (состояние здесь пока не причём
			// в разработке)
                        condition = 0;
			MPI_Send(&condition, 1, MPI_INT, i, 1, currentComm);
			MPI_Status st;
			// Получаем результат запроса в виде true или false
			MPI_Recv(&exitTask, 1, MPI_INT, i, 2, currentComm, &st);
									
			// Если узел готов отправить задачу на обработку
			if (exitTask) 
			{	
				// Получаем данные задачи
				int begin, end;
				MPI_Recv(&begin, 1, MPI_INT, i, 3, currentComm, &st);
				MPI_Recv(&end, 1, MPI_INT, i, 3, currentComm, &st);
				Task t(begin, end);
				//int length;
				//MPI_Recv(&length, 1, MPI_INT, i, 3, currentComm, &st);
				//char *buf = new char(length);
				//MPI_Recv(&buf, length, MPI_CHAR, i, 3, currentComm, &st);
				//Task t(buf);
				t.Run();
				//delete[] buf;
                                // Если задача от текущего узла была получена, берём задачи из
				// этого узла до тех пор, пока задачи на нём не будут исчерпаны
				i--;
			}
		}
                else if(i >= size) i--;
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
	while(countOfAsks < (size-1)*countOfWorkers || !serverFinished || changeComm)
	{
		MPI_Status st;
		// Получаем запрос от любого узла
		MPI_Recv(&cond, 1, MPI_INT, MPI_ANY_SOURCE, 1, currentComm, &st);
		if (cond == 0)
		{
			// Получаем номер этого узла
			int peer = st.MPI_SOURCE;
			// Флаг ответа на то, есть задачи в узле, или нет
			int send = 0;
			// Если в очереди есть задача, получаем её (тоже не отлажено)
			if (GetTask(&t))
			{
				int buf[2];
				buf[0] = t -> GetBegin();
				buf[1] = t -> GetEnd();
				send = 1;
				pthread_mutex_lock(&mutex);
				std:: cerr << "task send:: rank " << rank << " to " << peer << "\n"; 
				pthread_mutex_unlock(&mutex);
				// Отправляем сообщение о том, что можем отправить задачу и отправляем её
				MPI_Send(&send, 1, MPI_INT, peer, 2, currentComm);	
				MPI_Send(&(buf), 1, MPI_INT, peer, 3, currentComm);
				MPI_Send(&(buf)+1, 1, MPI_INT, peer, 3, currentComm);
		//			char *buf;
		//			int length;
		//			t->Serialize(&buf, &length);
		//			MPI_Send(&length, 1, MPI_INT, peer, 3, currentComm);
		//			MPI_Send(&buf, length, MPI_CHAR, peer, 3, currentComm);
					
					//Task.bufFree();
		//			delete[] buf;
			}
			// Собственные задачи кончились, запускаем счётчик ответов на все узлы
			else if (countOfAsks < (size-1)*countOfWorkers)
			{	// Отправляем сообщение о том, что задачи кончились
				MPI_Send(&send, 1, MPI_INT, peer, 2, currentComm);
				countOfAsks++;
			}
		}		//break;
		else if (cond == 1)
		{      		        
                        for (int j = 0; j < countOfWorkers - 1; j++)
                                MPI_Recv(&cond, 1, MPI_INT, rank, 1, currentComm, &st);			                   
        		currentComm = newComm;	
                        MPI_Comm_rank(currentComm, &rank);
                        MPI_Comm_size(currentComm, &size);
			changeComm = false;
		}
                else serverFinished = true;
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
	if (rank == 0)
	{
		std::ofstream fPort("port_name.txt");
		for (int i = 0; i < MPI_MAX_PORT_NAME; i++)
			fPort << port_name[i];
		fPort.close();
                 printf("server available at %s\n", port_name);
	}

	MPI_Comm_accept(port_name, MPI_INFO_NULL, 0, currentComm, &client);
	again = 2;
	
	MPI_Intercomm_merge(client, false, &newComm);
	if (size == 1) currentComm = newComm;
	else changeComm = true;
        MPI_Send (&again, 1, MPI_INT,rank, 1, currentComm); 
}
 
int main(int argc, char **argv)
{
	int tasksPerProcess = 5000000, fragmentSize;
	int lvl;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &lvl);
	MPI_Comm_rank(currentComm, &rank);
	MPI_Comm_size(currentComm, &size);
	MPI_Status st;
        
//      std::cerr << "level" << lvl << "\n";
	fragmentSize = globalTaskSize /(size*tasksPerProcess);

        int status = pthread_barrier_init(&barrier, NULL, countOfWorkers);
        if (status != 0) 
        {
                printf("main error: can't init barrier, status = %d\n", status);
                exit(ERROR_INIT_BARRIER);
        }

	// Формирование очереди
	for (int i = 0; i < tasksPerProcess; i++)
	{
		int begin = rank*fragmentSize*tasksPerProcess;
		tasksContainer.push(new Task(begin, begin + fragmentSize));
	}
//	if (rank == 0)
//	        for(int i = 0; i < globalTaskSize; i++)	
//		        tasksContainer.push(new Task(i, i+1));
///
	for (int i = 0; i < globalTaskSize; i++) 
		v1[i] = v2[i] = 1;
	
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

	if(0!=pthread_create(&thrs[2], &attrs, server, &ids[2]))
        {
                perror("Cannot create a thread");
                abort();
        }

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
	MPI_Reduce(&globalRes, &resultOfGlobalTask, 1, MPI_INT, MPI_SUM, 0, currentComm);
	if (rank == 0)
		std::cerr << "Global result = " << resultOfGlobalTask << "\n";
	MPI_Finalize();	
	return 0;
}

