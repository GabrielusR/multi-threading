#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <fstream>
#include <future>

#include "logging.h"

std::atomic<bool> isFinished = {false};

//template<typename T>
class ThreadSafeQueue {

private:

	std::vector<int> queue{};
	std::mutex mtx{};
	std::condition_variable cv{};

public:

	ThreadSafeQueue() = default;

	ThreadSafeQueue(size_t reserve) { queue.reserve(reserve); }

	void push(int val) {

		std::lock_guard<std::mutex> guard(mtx);

		DEBUG("push");

		queue.push_back(std::move(val));
		//create a notification for the thread waiting for this work to be done
		cv.notify_one();	
	}
	
	bool waitPop(int &val) {

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

int main() {

	ThreadSafeQueue tsQueue(1000);

	std::ofstream resLog("result.log", std::ios::app);

#if 0
	std::thread workerProd([&tsQueue]() {

		int i = {};

		//for(int i = 0; i < 1000; i++) {
		while(!isFinished)
			tsQueue.push(++i);
		//}
	});
	
	std::thread workerCons([&tsQueue, &resLog]() {

		int value = {};	

		while(!isFinished) {

			if(tsQueue.waitPop(value))
				LOG(resLog, value);//resLog << value << std::endl;
		}
	});
#endif

	std::future<void> prodFuture = std::async(std::launch::async, [&tsQueue]() {

		int i = {};

		//for(int i = 0; i < 1000; i++) {
		while(!isFinished)
			tsQueue.push(++i);
		//}
	});
	
	std::future<void> consFuture = std::async(std::launch::async, [&tsQueue, &resLog]() {

		int value = {};	

		while(!isFinished) {

			if(tsQueue.waitPop(value))
				LOG(resLog, value);//resLog << value << std::endl;
		}
	});

	std::cin.get();

	tsQueue.stop();
	//workerProd.join();
	//workerCons.join();

	PRINT("Main thread finished.");

	return 0;
}
