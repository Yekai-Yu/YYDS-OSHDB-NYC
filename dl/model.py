import os
import gc
import random
import time; _START_RUNTIME = time.time()
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

from sklearn.metrics import r2_score, mean_squared_error
from torch.utils.data import Dataset
from torch.utils.data import DataLoader

seed = 24
random.seed(seed)
np.random.seed(seed)
torch.manual_seed(seed)
os.environ["PYTHONHASHSEED"] = str(seed)
N_feature = 23
BATCH_SIZE = 16
N_EPOCH = 20
PRINT_INTERVAL = 10
N_LABEL = 8
LABELS = ["PUAM", "PUPM", "PUMD", "PUEV", "DOAM", "DOPM", "DOMD", "DOEV"]

class Dataset(Dataset):
    def __init__(self, data_path):
        f = open(data_path, "r")
        self.x = []
        self.y = []
        for idx, line in enumerate(f):
            if idx==0:
                continue
            l = line.strip('\n').split("\t")
            ## y
            self.y.append([int(x) for x in l[4:12]])
            ## x
            time = [0]*19
            year = int(l[1])
            month = int(l[2])
            weekday = int(l[3])
            time[year-2014] = 1
            time[month-1+6] = 1
            time[-1] = weekday
            inp = time+[float(x) for x in (l[12:len(l)])]
            self.x.append(inp)

    def __len__(self):
        return(len(self.y))
    def __getitem__(self, index):
        return(torch.FloatTensor(self.x[index]), torch.FloatTensor(self.y[index]))

class Net(nn.Module):
    def __init__(self, in_feature):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(in_feature, 16)
        #self.fc2 = nn.Linear(64, 32)
        self.dropout = nn.Dropout(p=0.5)
        self.fc3 = nn.Linear(16, 8)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        #x = torch.relu(self.fc2(x))
        x = self.dropout(x)
        x = torch.relu(self.fc3(x))
        return x

def train_model(model, train_loader, val_loader, n_epochs, logfile):
    t1 = time.time()
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.1,
                                               patience=1, verbose=True, threshold=1e-4,
                                               threshold_mode='rel', cooldown=0, min_lr=0, eps=1e-08)
    
    # prep model for training
    model.train()

    train_loss_arr = []
    val_loss_arr = []
    lr_arr = []

    log = open(logfile, "a")
    log.write("\n\n\nStarted training, total epoch : {}\n".format(n_epochs))
    log.write("Training data size: {}\n".format(len(train_loader)))
    print("Started training, total epoch : {}".format(n_epochs))
    print("Training data size: {}".format(len(train_loader)))

    for epoch in range(n_epochs):
        gc.collect()
        torch.cuda.empty_cache()
        train_loss = 0
        batch = 0
        log.write("\nStarted epoch {}\n".format(epoch+1))
        print("\nStarted epoch {}".format(epoch+1))

        for x, y in train_loader:
            optimizer.zero_grad()
            y_hat = model(x)
            loss = criterion(y_hat, y)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()
            if ((batch+1) % PRINT_INTERVAL == 0):
                log.write('Trained {} batches \tTraining Loss: {:.6f}\n'.format(batch+1, loss.item()))
                print('Trained {} batches \tTraining Loss: {:.6f}'.format(batch+1, loss.item()))
            batch += 1

        train_loss = train_loss / len(train_loader)
        train_loss_arr.append(np.mean(train_loss))
        torch.save(model.state_dict(), str(epoch+1)+"trained.pth")

        log.write('AUROCs on validation dataset:\n')
        print('AUROCs on validation dataset:')
        log.close()
        gc.collect()
        torch.cuda.empty_cache()
        model.eval()
        val_loss = 0       
        with torch.no_grad():
            val_loss = eval_model(model, val_loader, logfile, "validation")
        val_loss_arr.append(np.mean(val_loss))
        lr_arr.append(optimizer.param_groups[0]['lr'])

        log = open(logfile, "a")
        log.write('Epoch {} Statistics:\nTraining Loss: {:.6f}\nValidation Loss: {:.6f}\n'.format(epoch+1, train_loss, val_loss))
        print('Epoch {} Statistics:\nTraining Loss: {:.6f}\nValidation Loss: {:.6f}'.format(epoch+1, train_loss, val_loss))
        log.write('Epoch: {} \tLearning Rate for first group: {:.10f}\n'.format(epoch+1, optimizer.param_groups[0]['lr']))
        print('Epoch: {} \tLearning Rate for first group: {:.10f}'.format(epoch+1, optimizer.param_groups[0]['lr']))
        model.train()
        scheduler.step(val_loss)

    t2 = time.time()
    log.write("\nTrain, Val Loss & Learning Rate by Epoch:\n")
    for i in range(n_epochs):
        log.write("Epoch {}: {:.6f} {:.6f} {:.10f}\n".format(i+1, train_loss_arr[i], val_loss_arr[i], lr_arr[i]))
    log.write("Training time lapse: {} min\n\n\n".format((t2 - t1) // 60))
    print("Training time lapse: {} min\n".format((t2 - t1) // 60))
    log.close()

def eval_model(model, test_loader, logfile, setstr):
    log = open(logfile, "a")
    
    criterion = nn.MSELoss()
    test_loss = 0
    y_test = torch.FloatTensor()
    #y_test = y_test.cuda()
    y_pred = torch.FloatTensor()
    #y_pred = y_pred.cuda()
    log.write("Evaluating {} data...\t {}_loader: {}\n".format(setstr, setstr, len(test_loader)))
    print("Evaluating {} data...\t {}_loader: {}".format(setstr, setstr, len(test_loader)))
    t1 = time.time()
    for i, (x, y) in enumerate(test_loader):
        #y = y.cuda()
        y_test = torch.cat((y_test, y), 0)
        #_, channel, height, width= x.size()
        #with torch.no_grad():
        #    x_in = torch.autograd.Variable(x.view(-1, channel, height, width))#.cuda())
        #y_hat = model(x_in)
        y_hat = model(x)
        y_pred = torch.cat((y_pred, y_hat), 0)
        loss = criterion(y_pred, y_test)
        test_loss += loss.item()
        if (i % PRINT_INTERVAL == 0):
            log.write("batch: {}\n".format(i))
            print("batch: {}".format(i))
    t2 = time.time()
    test_loss = test_loss / len(test_loader)

    log.write("Evaluating time lapse: {} min\n".format((t2 - t1) // 60))
    print("Evaluating time lapse: {} min".format((t2 - t1) // 60))
    log.write('Loss on {} dataset: {:.6f}\n'.format(setstr, test_loss))
    print('Loss on {} dataset: {:.6f}'.format(setstr, test_loss))

    """Compute R2 & MSE for each time period"""
    R2 = []
    MSE = []
    y_test_np = y_test #.cpu().detach().numpy()
    y_pred_np = y_pred #.cpu().detach().numpy()
    for i in range(N_LABEL):
        result = r2_score(y_test_np[:, i], y_pred_np[:, i])
        R2.append(result)
        result = mean_squared_error(y_test_np[:, i], y_pred_np[:, i])
        MSE.append(result)

    R2_avg = np.array(R2).mean()
    MSE_avg = np.array(MSE).mean()

    log.write('The average R2 is {:.6f}\n'.format(R2_avg))
    print('The average R2 is {:.6f}'.format(R2_avg))
    log.write('The average MSE is {:.6f}\n'.format(MSE_avg))
    print('The average MSE is {:.6f}'.format(MSE_avg))
    for i in range(N_LABEL):
        log.write('The R2 of {} is {}\n'.format(LABELS[i], R2[i]))
    for i in range(N_LABEL):
        log.write('The MSE of {} is {}\n'.format(LABELS[i], MSE[i]))

    log.close()
    return test_loss

model = Net(N_feature)

""" Initialize the Dataset"""
train_dataset = Dataset("final_train.csv")
val_dataset = Dataset("final_val.csv")
test_dataset = Dataset("final_test.csv")

train_loader = DataLoader(dataset=train_dataset, batch_size=BATCH_SIZE, shuffle=True)
val_loader = DataLoader(dataset=val_dataset, batch_size=BATCH_SIZE, shuffle=False)
test_loader = DataLoader(dataset=test_dataset, batch_size=BATCH_SIZE, shuffle=False)

print(len(train_dataset))
print(len(val_dataset))
print(len(test_dataset))

logfile = "runlog.txt"


""" Training the Model """
train_model(model, train_loader, val_loader, N_EPOCH, logfile)


""" Evaluating the Model 
The following part evaluated the model trained after the last epoch by default.
To evaluate the model with the best performance, one can load the corresponding model and skip the training procedure.
"""
gc.collect()
torch.cuda.empty_cache()
model.eval()
with torch.no_grad():
    eval_model(model, test_loader, logfile, "test (last epoch)")