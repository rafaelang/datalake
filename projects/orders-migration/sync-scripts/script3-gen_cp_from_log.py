import argparse
import glob
import re 


def find_s3_urls(string):
    pattern = 's3[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    return re.findall(pattern, string) 


def gen_cp_files(logs_dir, output_script):
    logs_files = glob.glob(logs_dir + "/*")
    for log_file in logs_files:
        with open(log_file) as in_file:
            with open(output_script, 'a') as out_file:
                for _, line in enumerate(in_file):
                    s3_urls = find_s3_urls(line)
                    if len(s3_urls) > 0:
                        try:
                            src_file_s3 = s3_urls[0]
                            target_path_s3 = s3_urls[1]
                            commmand = "aws s3 cp " + src_file_s3 + " " + target_path_s3 + "\n"
                            out_file.write(commmand)
                        except:
                            print(line)
                    else:
                        print(line)
                       

def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--logs-dir', 
        required=True
    )
    parser.add_argument(
        '--output-script', 
        required=True
    )
    args=parser.parse_args()
    
    return args.logs_dir, args.output_script


if __name__ == "__main__":
    logs_dir, output_script = _read_args()
    gen_cp_files(logs_dir, output_script)