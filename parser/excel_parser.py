import pandas as pd

# 로그 파일 경로 설정
log_file_path = "sdc_original_latest_log_waiting_time"

# 데이터를 저장할 리스트 초기화
data = []

# 로그 파일을 읽어서 데이터 추출
with open(log_file_path, "r") as log_file:
    for line in log_file:
        # 공백으로 분리된 값을 읽어옴
        values = line.strip().split()
        # 데이터를 리스트에 추가
        data.append([int(values[0]), float(values[1]), int(values[2])])

# 데이터를 DataFrame으로 변환
df = pd.DataFrame(data, columns=["job_id", "wait_time", "target_queue"])

# 엑셀 파일로 저장
df.to_excel("sdc_original_latest_log_waiting_time.xlsx", index=False)

print("데이터가 엑셀 파일로 저장되었습니다.")