count = 0

# 파일을 열고 각 줄을 읽는다
with open('2nd_log_ms', 'r') as file:
    for line in file:
        # 줄을 공백으로 구분하여 리스트로 변환
        numbers = line.split()

        # 마지막 숫자가 '0'인지 확인
        #if numbers[-1] == '0':
         #   count += 1

        num = int(numbers[-1])

        if num >= 0 and num <= 1000:
            count += 1


print(f"0인 경우의 수: {count}")

# 2nd 이상 sec => 9908 /

# sec : 0인 것의 갯수
# ms : 0 ~ 1000ms 사이인 것의 갯수

# 2nd 실제 sec => 9980 / 22948
# 2nd 실제 ms => 4818 / 22948

# 2nd synth 50000 sec => 34059 / 54927
# 2nd synth 50000 ms => 19749 / 54927

# 2nd synth 100000 sec => 68023 / 109856
# 2nd synth 100000 ms => 39345 / 109856

# 4th 실제 sec => 8769 / 22467
# 4th 실제 ms => 14073 / 22467