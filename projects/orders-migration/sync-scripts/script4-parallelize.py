import argparse
import glob
import re 


def parellalize_script(script_path, output_script_path):
    counter = 1
    switched = True
    with open(script_path) as in_file:
        with open(output_script_path, 'a') as out_file:
            for _, line in enumerate(in_file):
                posfix = " "
                if counter == 11:
                    counter = 1
                    switched = not switched
                if switched or (not switched and counter > 1):
                    posfix = " &"
                    switched = True

                commmand = line.strip() + posfix + "\n"
                out_file.write(commmand)
                counter += 1
                       

def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--input-script', 
        required=True
    )
    parser.add_argument(
        '--output-script', 
        required=True
    )
    args=parser.parse_args()
    
    return args.input_script, args.output_script


if __name__ == "__main__":
    input_script, output_script = _read_args()
    parellalize_script(input_script, output_script)