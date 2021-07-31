
f = open("final-normalized.csv", "r")
fw = open("final-ml.csv", "w")

idx = 0
for line in f:
    if idx==0:
        label = line.strip("\n").split(",")
        print(label)
        newlabel = label[4:12]+["year2014","year2015","year2016","year2017","year2018","year2019",
                    "month1", "month2", "month3", "month4", "month5", "month6", 
                    "month7", "month8", "month9", "month10", "month11", "month12",
                    "weekday"] + label[12:len(label)]
        print(newlabel)
        fw.write(",".join(newlabel)+"\n")
        idx = idx+1
        continue
    
    l = line.strip('\n').split(",")
      
    y = [x for x in l[4:12]]
            
    time = ["0"]*19
    year = int(l[1])
    month = int(l[2])
    weekday = l[3]
    time[year-2014] = "1"
    time[month-1+6] = "1"
    time[-1] = weekday

    inp = y+time+[x for x in (l[12:len(l)])]
    fw.write(",".join(inp)+"\n")
    idx = idx+1

f.close()
fw.close()