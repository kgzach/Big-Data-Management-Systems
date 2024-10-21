import os
import time
import subprocess

def run_script(script_name):
    script_path = os.path.join('scripts', script_name)
    try:
        result = subprocess.run(
            ['python3', script_path],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Output of {script_name}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e.stderr}")
        exit(1)
    except FileNotFoundError:
        print(f"Script {script_name} not found.")
        exit(1)

if __name__ == '__main__':
    run_script('broker.py') # success
    time.sleep(5)
    run_script('producer.py')
    time.sleep(5)
    run_script('consumer.py')
    time.sleep(5)
    run_script('spark_dataframe.py')
