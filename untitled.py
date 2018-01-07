def updateNewEmptyStation(newValues, tracker):
    if tracker is None:
        tracker = 1
        print(newValues[len(newValues)-1])
    elif newValues[len(newValues)-1] == 0 and tracker ==1:
        tracker = 0
    else:
        tracker = 1
    return tracker

    fff


    