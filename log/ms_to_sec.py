import math

# 새로운 내용을 저장할 리스트
new_lines = []

with open('2nd_log_ms', 'r') as file:
    for line in file:
        # 줄을 공백으로 구분하여 숫자 리스트로 변환
        numbers = [int(num) for num in line.split()]

        # 3번째와 5번째 숫자를 10000으로 나누고 math.floor() 적용
        numbers[2] = math.floor(numbers[2] / 10000)
        numbers[4] = math.floor(numbers[4] / 10000)

        # 수정된 줄을 문자열로 변환하여 새 리스트에 추가
        new_line = ' '.join(map(str, numbers))
        new_lines.append(new_line)

# 새로운 내용을 b.txt 파일에 쓴다
with open('2nd_log_sec', 'w') as new_file:
    for line in new_lines:
        new_file.write(line + '\n')
