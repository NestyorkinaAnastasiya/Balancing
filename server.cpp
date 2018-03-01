#include <mpi.h>
#define HAVE_STRUCT_TIMESPEC
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>
#include <vector>
#define MAX_DATA 1000

#define ERROR_CREATE_THREAD   -11
#define ERROR_JOIN_THREAD     -12
#define BAD_MESSAGE           -13
#define ERROR_INIT_BARRIER    -14
#define ERROR_WAIT_BARRIER    -15
#define ERROR_DESTROY_BARRIER -16
#define SUCCESS                 0

// Размер задачи
const int globalTaskSize = 100;
// Количество задач на один процесс
int tasksPerProcess = 100;
// Векторы, которые предстоит умножить
int v1[globalTaskSize], v2[globalTaskSize];

int globalRes = 0, resultOfGlobalTask = 0;

int ids[7] = { 0,1,2,3,4,5,6 };

pthread_t thrs[4];
pthread_barrier_t barrier;
int rank, rank_old = 0, size, size_old = 0;
int count_change = 0;

// Текущий коммуникатор
MPI_Comm currentComm = MPI_COMM_WORLD;
// Новый коммуникатор
MPI_Comm newComm;

int countOfWorkers = 4;
int countOfThreads = 6;

int condition = 0;
bool changeComm = false;
bool serverFinished = false;

pthread_mutex_t mutex;

class Task
{
	// Начало и конец обработки вектора
	int beg, end;
public:
	Task(int b, int e) :beg(b), end(e) {}
	void Run();
	// Начало подзадачи
	int GetBegin() { return beg; }
	// Конец подзадачи
	int GetEnd() { return end; }
};

void Task::Run()
{
	int res = 0;
	for (int i = beg; i < end; i++)
		res += v1[i] * v2[i];
	// захват мьютекса
	pthread_mutex_lock(&mutex);
	// добавление к глобальному результату при исключительном владении глобальной переменной
	globalRes += res;
	// освобождение мьютекса
	pthread_mutex_unlock(&mutex);
}

std::queue<Task*> QueueOfTasks;

bool GetTask(Task **currTask)
{
	pthread_mutex_lock(&mutex);

	if (QueueOfTasks.empty())
	{
		pthread_mutex_unlock(&mutex);
		return false;
	}
	else
	{
		*currTask = QueueOfTasks.front();
		QueueOfTasks.pop();
	}

	pthread_mutex_unlock(&mutex);
	return true;
}

// Функция вычислительного потока
void* worker(void* me)
{
	Task *currTask;
	int  exitTask = 0;
	bool message = false;
	MPI_Comm Comm = currentComm;

	// Пока есть свои задачи - выполняем свои
	while (GetTask(&currTask))
	{
		// Если происходит изменение коммуникатора
		if (changeComm)
		{
			// И сообщение о смене коммуникатора ещё не отправлено
			if (!message)
			{
				for (int i = 0; i < size; i++)
				{
					condition = 3;
					// Отправка сообщения диспечеру со старым коммуникатором о том, 
					// что коммуникатор сменён
					MPI_Send(&condition, 1, MPI_INT, i, 1, Comm);
					Comm = newComm;
					message = true;
				}
			}
		}
		else message = false;

		currTask->Run();
	}

	// Запрашиваем по одной задаче от каждого узла кроме самого себя
	for (int i = 0; i < size; i++)
	{
		// Если происходит изменение коммуникатора
		if (changeComm)
		{
			// И сообщение о смене коммуникатора ещё не отправлено
			if (!message)
			{
				for (int i = 0; i < size; i++)
				{
					condition = 3;
					// Отправка сообщения диспечеру со старым коммуникатором о том, 
					// что коммуникатор сменён
					MPI_Send(&condition, 1, MPI_INT, i, 1, Comm);
					Comm = newComm;
					message = true;
				}
			}
		}
		else message = false;

		if (i != rank)
		{	// Отправляем запрос на получение задачи 
			condition = 0;
			MPI_Send(&condition, 1, MPI_INT, i, 1, Comm);
			MPI_Status st;
			// Получаем результат запроса в виде true или false
			MPI_Recv(&exitTask, 1, MPI_INT, i, 2, Comm, &st);

			// Если узел готов отправить задачу на обработку
			if (exitTask)
			{
				// Получаем данные задачи
				int begin, end;
				MPI_Recv(&begin, 1, MPI_INT, i, 3, Comm, &st);
				MPI_Recv(&end, 1, MPI_INT, i, 3, Comm, &st);
				Task t(begin, end);

				t.Run();

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
	return 0;
}
//Диспетчер для работы в старом коммуникаторе
void* dispatcher_old(void* me)
{
	Task *t;
	int cond;
	int countOfAsks = 0;

	// Пока не получены сообщения о смене коммуникатора от макс кол-ва вычислительных потоков
	while (count_change != rank_old * countOfWorkers)
	{
		MPI_Status st;
		// Получаем запрос от любого узла
		MPI_Recv(&cond, 1, MPI_INT, MPI_ANY_SOURCE, 1, currentComm, &st);

		// Если это запрос о получении задачи
		if (cond == 0)
		{
			// Получаем номер этого узла
			int peer = st.MPI_SOURCE;
			// Флаг ответа на то, есть задачи в узле, или нет
			int send = 0;

			// Если в очереди есть задача, получаем её
			if (GetTask(&t))
			{
				int buf[2];
				buf[0] = t->GetBegin();
				buf[1] = t->GetEnd();
				send = 1;
				pthread_mutex_lock(&mutex);
				std::cerr << "task send:: rank " << rank << " to " << peer << "\n";
				pthread_mutex_unlock(&mutex);

				// Отправляем сообщение о том, что можем отправить задачу и отправляем её
				MPI_Send(&send, 1, MPI_INT, peer, 2, currentComm);
				MPI_Send(&(buf), 1, MPI_INT, peer, 3, currentComm);
				MPI_Send(&(buf)+1, 1, MPI_INT, peer, 3, currentComm);
			}
			// Собственные задачи кончились, запускаем счётчик ответов на все узлы
			else if (countOfAsks < (size - 1)*countOfWorkers)
			{
				// Отправляем сообщение о том, что задачи кончились
				MPI_Send(&send, 1, MPI_INT, peer, 2, currentComm);
				countOfAsks++;
			}
		}
		if (cond == 3)
		{
			count_change++;
		}
	}

	currentComm = newComm;
	changeComm = false;
	return 0;
}

// Диспетчер, который работает всегда в новом коммуникаторе
void* dispatcher(void* me)
{
	Task *t;
	int cond;
	int countOfAsks = 0;
	MPI_Comm Comm = currentComm;

	// Пока не получены запросы задач от макс кол-ва вычислительных потоков
	while (countOfAsks < (size - 1)*countOfWorkers)
	{
		MPI_Status st;
		// Получаем запрос от любого узла
		MPI_Recv(&cond, 1, MPI_INT, MPI_ANY_SOURCE, 1, currentComm, &st);

		// Если это запрос о получении задачи
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
				buf[0] = t->GetBegin();
				buf[1] = t->GetEnd();
				send = 1;

				pthread_mutex_lock(&mutex);
				std::cerr << "task send:: rank " << rank << " to " << peer << "\n";
				pthread_mutex_unlock(&mutex);

				// Отправляем сообщение о том, что можем отправить задачу и отправляем её
				MPI_Send(&send, 1, MPI_INT, peer, 2, Comm);
				MPI_Send(&(buf), 1, MPI_INT, peer, 3, Comm);
				MPI_Send(&(buf)+1, 1, MPI_INT, peer, 3, Comm);
			}
			// Собственные задачи кончились, запускаем счётчик ответов на все узлы
			else
			{
				// Отправляем сообщение о том, что задачи кончились
				MPI_Send(&send, 1, MPI_INT, peer, 2, Comm);
				countOfAsks++;
			}
		}
		else if (cond == 1)
		{
			rank_old = rank;
			size_old = size;

			// Атрибуты потока
			pthread_attr_t attrs;

			// Инициализация атрибутов потока
			if (0 != pthread_attr_init(&attrs))
			{
				perror("Cannot initialize attributes");
				abort();
			};

			// Порождение диспетчера, работающего в старом коммуникаторе
			if (0 != pthread_create(&thrs[countOfWorkers + 2], &attrs, dispatcher_old, &ids[countOfWorkers + 2]))
			{
				perror("Cannot create a thread");
				abort();
			}

			//Меняем текущий коммуникатор на новый                  
			Comm = newComm;

			// Вычисляем новый размер и rаnk
			MPI_Comm_rank(Comm, &rank);
			MPI_Comm_size(Comm, &size);

			changeComm = true;
		}
	}

	// Сообщение об окончании работы диспетчера (отладка)
	std::cerr << "rank " << rank << ":: despatcher closed.\n";
	return 0;
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
	again = 1;

	MPI_Intercomm_merge(client, false, &newComm);

	MPI_Send(&again, 1, MPI_INT, rank, 1, currentComm);
	return 0;
}

int main(int argc, char **argv)
{
	int fragmentSize;
	int lvl;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &lvl);
	MPI_Comm_rank(currentComm, &rank);
	MPI_Comm_size(currentComm, &size);
	MPI_Status st;

	//      std::cerr << "level" << lvl << "\n";
	fragmentSize = globalTaskSize / (size*tasksPerProcess);

	int status = pthread_barrier_init(&barrier, NULL, countOfWorkers);
	if (status != 0)
	{
		printf("main error: can't init barrier, status = %d\n", status);
		exit(ERROR_INIT_BARRIER);
	}

	// Инициализация мьютекса
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutex_init(&mutex, &attr);

	// Формирование очереди задач
	int begin = rank * fragmentSize*tasksPerProcess;
	for (int i = 0; i < tasksPerProcess; i++)
	{
		QueueOfTasks.push(new Task(begin, begin + fragmentSize));
		begin += fragmentSize;
	}
	//	if (rank == 0)
	//	        for(int i = 0; i < globalTaskSize; i++)	
	//		        QueueOfTasks.push(new Task(i, i+1));
	///
	for (int i = 0; i < globalTaskSize; i++)
		v1[i] = v2[i] = 1;

	// Атрибуты потока
	pthread_attr_t attrs;

	// Инициализация атрибутов потока
	if (0 != pthread_attr_init(&attrs))
	{
		perror("Cannot initialize attributes");
		abort();
	};
	// Установка атрибута "присоединенный"
	if (0 != pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE))
	{
		perror("Error in setting attributes");
		abort();
	}
	// Порождение вычислительных потоков
	for (int i = 0; i < countOfWorkers; i++)
		if (0 != pthread_create(&thrs[i], &attrs, worker, &ids[i]))
		{
			perror("Cannot create a thread");
			abort();
		}

	// Порождение диспетчера
	if (0 != pthread_create(&thrs[countOfWorkers], &attrs, dispatcher, &ids[countOfWorkers]))
	{
		perror("Cannot create a thread");
		abort();
	}

	// Порождение сервера
	/*if (0 != pthread_create(&thrs[countOfWorkers + 1], &attrs, server, &ids[countOfWorkers + 1]))
	{
		perror("Cannot create a thread");
		abort();
	}*/

	//освобождение ресурсов, занимаемых описателем атрибутов
	pthread_attr_destroy(&attrs);
	pthread_mutexattr_destroy(&attr);

	//ожидание завершения порожденных потоков
	for (int i = 0; i < countOfThreads-1; i++)
		if (0 != pthread_join(thrs[i], NULL))
		{
			perror("Cannot join a thread");
			abort();
		}
	std::cerr << "threads finished\n";

	MPI_Reduce(&globalRes, &resultOfGlobalTask, 1, MPI_INT, MPI_SUM, 0, currentComm);

	if (rank == 0)
		std::cerr << "Global result = " << resultOfGlobalTask << "\n";
	MPI_Finalize();

	return 0;
}
