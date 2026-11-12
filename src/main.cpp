#include <iostream>
#include <vector>
#include <fstream>

#include <future>
#include <thread>

#include <mutex>
#include <condition_variable>
#include <atomic>

#include <functional>

#include "logging.h"

#ifndef THREAD
#define THREAD 1
#endif

std::atomic<bool> isFinished = {false};

template<typename T>
class ThreadSafeQueue {

private:

	std::vector<T> queue{};
	std::mutex mtx{};
	std::condition_variable cv{};

public:

	ThreadSafeQueue() = default;

	explicit ThreadSafeQueue(size_t reserve) { queue.reserve(reserve); }

	void push(T val) {

		std::lock_guard<std::mutex> guard(mtx);

		DEBUG("push");

		queue.push_back(std::move(val));

		//create a notification for the thread waiting for this work to be done
		cv.notify_one();	
	}
	
	bool waitPop(T &val) {

		std::unique_lock<std::mutex> guard(mtx);

		cv.wait(guard, [this](){ return !queue.empty() || isFinished; });

		DEBUG("waitPop");

		if(queue.empty() && isFinished) return false;

		val = std::move(queue.front());
		queue.erase(queue.begin());

		return true;
	}

	void stop() {
		
		std::lock_guard<std::mutex> guard(mtx);

		DEBUG("stop");

		isFinished = true;

		cv.notify_all();
	}
};

#if THREAD 
void producer(ThreadSafeQueue<int> &tsQueue, std::promise<int>& prodRes) {

	try {
		int i = {};

		while(!isFinished)
			tsQueue.push(++i);
		
		prodRes.set_value(0);

	} catch(...) {

		prodRes.set_exception(std::current_exception());
	}
}

void consumer(ThreadSafeQueue<int> &tsQueue, std::ofstream& resLog, std::promise<int>& consRes) {

	try {
		int value = {};	

		while(tsQueue.waitPop(value))
			LOG(resLog, value);

		consRes.set_value(0);

	} catch(...) {

		consRes.set_exception(std::current_exception());	
	}
}
#endif

int main() {

	ThreadSafeQueue<int> tsQueue(1000);

	std::ofstream resLog("result.log", std::ios::app);

#if THREAD 

	std::promise<int> prodRes{};
	std::future<int> prodFuture = prodRes.get_future();

	std::promise<int> consRes{};
	std::future<int> consFuture = consRes.get_future();

	std::thread workerProd(producer, std::ref(tsQueue), std::ref(prodRes));	
	std::thread workerCons(consumer, std::ref(tsQueue), std::ref(resLog), std::ref(consRes));

#else
	std::future<int> prodFuture = std::async(std::launch::async, [&tsQueue]() {

		int i = {};

		while(!isFinished)
			tsQueue.push(++i);
		
		return 0;
	});
	
	std::future<int> consFuture = std::async(std::launch::async, [&tsQueue, &resLog]() {

		int value = {};	

		while(tsQueue.waitPop(value))
			LOG(resLog, value);

		return 0;
	});
#endif

	std::cin.get();
	tsQueue.stop();

#if THREAD
	workerProd.join();
	workerCons.join();

	DEBUG("Versão THREAD + PROMISE + FUTURE");
#else

	DEBUG("Versão ASYNC + FUTURE");
#endif

	PRINT(prodFuture.get());
	PRINT(consFuture.get());

	PRINT("Main thread finished.");

	return 0;
}
