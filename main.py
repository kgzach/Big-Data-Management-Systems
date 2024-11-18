import os
import time
import subprocess
from dotenv import load_dotenv
from multiprocessing import Process, Event


def run_script(script_name):
    script_path = os.path.join('scripts', script_name)
    try:
        print("Starting " + script_path)
        # Using pipe in order to print the subprocesses output in terminal
        with subprocess.Popen(
            ['python3', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        ) as proc:
            print("-" * 50)
            for line in proc.stdout:
                print(f"{script_name} output: {line}", end='')
            for line in proc.stderr:
                print(f"{script_name} error: {line}", end='')
    except subprocess.CalledProcessError as e:
        print("$" * 50)
        print(f"Error running {script_name}: {e.stderr}")
        exit(1)
    except FileNotFoundError:
        print("$" * 50)
        print(f"Script {script_name} not found.")
        exit(1)

def run_mongo_query(stopEvent):
    """Individual function, query process needs to sleep for 2 minutes
    in order to get some new data in database"""
    while not stopEvent.is_set(): # in order to stop the new mongo_query process from terminal
        mongo_query = Process(target=run_script, args=('query_mongo.py',))
        mongo_query.start()
        mongo_query.join()
        time.sleep(120)


if __name__ == '__main__':
    load_dotenv()
    producer_process = Process(target=run_script, args=('producer.py',))
    consumer_process = Process(target=run_script, args=('consumer.py',))
    spark_process = Process(target=run_script, args=('spark_dataframe.py',))
    stop_event = Event()

    run_script('broker.py')
    producer_process.start()
    consumer_process.start()
    spark_process.start()

    mongo_query_process = Process(target=run_mongo_query, args=(stop_event,))
    mongo_query_process.start()
    try:
        while True:
            producer_process.join(timeout=1)
            consumer_process.join(timeout=1)
            spark_process.join(timeout=1)
    except KeyboardInterrupt:
        print("\nShutting down processes...")
        stop_event.set()
        producer_process.terminate()
        consumer_process.terminate()
        spark_process.terminate()
        mongo_query_process.terminate()

    producer_process.join()
    consumer_process.join()
    spark_process.join()
    mongo_query_process.join()
    print("All processes have been terminated.")
