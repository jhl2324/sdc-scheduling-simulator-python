input_file_path = "log3"
output_file_path = "../log/4th_log_sec"

replacement_dict = {"A": "0", "B": "1", "C": "2", "D": "3"}

with open(input_file_path, "r") as input_file:
    with open(output_file_path, "w") as output_file:
        for line in input_file:
            for key, value in replacement_dict.items():
                line = line.replace(key, value)
            output_file.write(line)

print("done")