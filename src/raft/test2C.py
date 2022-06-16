from http.client import FAILED_DEPENDENCY
import subprocess
for i in range (0,1000):
    if i%10==0:
        print("testcase:", i)
    if i ==90 :
        print("Congrats!")
    #command = "export GO111MODULE=\"off\" && go test -run TestFailNoAgree2B && go test -run TestFailAgree2B && go test -run TestRPCBytes2B && go test -run TestBasicAgree2B && go test -run TestConcurrentStarts2B && go test -run TestRejoin2B"
    command = "export GO111MODULE=\"off\" && go test -run 2C -race"
    p = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    if "PASS" in stdout.decode():
        continue
    if "fail" in stdout.decode():
        print("Failed!\n")
        print(stdout.decode())
        break
    if "warning" in stdout.decode():
        print("Warning!")
        print(stdout.decode())
        continue
    else:
        print("Failed!\n")
        print(stdout.decode())
        break
