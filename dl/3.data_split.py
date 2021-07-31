import random

random.seed(10086)
N = 26012

ls =[i for i in range(1,1+N)]
#print(ls)
random.shuffle(ls)
cp1 = int(N*0.8)
cp2 = int(N*0.9)
lstrain = ls[:cp1]
lsval = ls[cp1:cp2]
lstest = ls[cp2:]
#print(lstrain, lsval, lstest)

f = open("final-ml.csv", "r")
f_train = open("final_train.csv", "w")
f_val = open("final_val.csv", "w")
f_test = open("final_test.csv", "w")
count = 0
for line in f:
    if count==0:
        f_train.write(line)
        f_val.write(line)
        f_test.write(line)
    elif count in lstrain:
        f_train.write(line)
    elif count in lsval:
        f_val.write(line)
    else:
        f_test.write(line)
    count += 1

f.close()
f_train.close()
f_val.close()
f_test.close()

