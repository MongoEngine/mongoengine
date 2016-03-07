import argparse
import subprocess
import os
import json


def run_test(test):
    command = ['python', test]

    result = subprocess.Popen(command,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    out, err = result.communicate()

    # In case we execute something thats not a unit test
    test_duration = '0s'
    try:
        test_duration = err.split('\n')[-4].split('in ')[1]
    # pylint: disable=bare-except
    except:
        pass

    test_info = {
        'name'         : test,
        'return_code'  : result.returncode,
        'output'       : out + err,
        'test_duration': test_duration
    }

    return test_info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dirs', nargs='*')
    args = parser.parse_args()

    test_files = []
    # Collect tests
    for dir_ in args.dirs:
        for file_ in os.listdir(dir_):
            file_path = os.path.join(dir_, file_)
            if os.path.isfile(file_path) and not file_.startswith('__') \
                    and file_.endswith('.py'):
                test_files.append(file_path)

    tests_info = [run_test(test) for test in test_files]

    print json.dumps(tests_info)

if __name__ == "__main__":
    main()
