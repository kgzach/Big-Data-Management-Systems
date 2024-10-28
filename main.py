import os
import time
import subprocess
from multiprocessing import Process

def run_script(script_name):
    script_path = os.path.join('scripts', script_name)
    try:
        print("Starting " + script_path)
        with subprocess.Popen(
            ['python3', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        ) as proc:
            for line in proc.stdout:
                print(f"{script_name} output: {line}", end='')
            for line in proc.stderr:
                print(f"{script_name} error: {line}", end='')
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e.stderr}")
        exit(1)
    except FileNotFoundError:
        print(f"Script {script_name} not found.")
        exit(1)

if __name__ == '__main__':
    producer_process = Process(target=run_script, args=('producer.py',))
    consumer_process = Process(target=run_script, args=('consumer.py',))
    spark_process = Process(target=run_script, args=('spark_dataframe.py',))
    mongo_process = Process(target=run_script, args=('query_mongo.py',))

    run_script('broker.py')

    time.sleep(2)
    producer_process.start()

    time.sleep(1)
    consumer_process.start()

    time.sleep(1)
    spark_process.start()

    time.sleep(60)
    mongo_process.start()
    try:
        while True:
            producer_process.join(timeout=1)
            consumer_process.join(timeout=1)
            spark_process.join(timeout=1)
            mongo_process.join(timeout=1)

    except KeyboardInterrupt:
        print("\nShutting down processes...")
        producer_process.terminate()
        consumer_process.terminate()
        spark_process.terminate()

    producer_process.join()
    consumer_process.join()
    spark_process.join()
    mongo_process.join()

    print("All processes have been terminated.")
