from pycompss.api.task import task

@task(returns=int)
def increment(v):
    return v+1

def main():
    from pycompss.api.api import compss_wait_on
    values = [0, 100, 200, 300]
    for i in range(4):
        for j in range(100):
            values[i] = increment(values[i])
    result = compss_wait_on(values)

    if result[0] == 100 and result[1] == 200 and result[2] == 300 and result[3] == 400:
        print "- Result value: OK"
    else:
        print "- Result value: ERROR"
        print "- This error is a root error. Please fix errors at test 19."

    raise Exception('GENERAL EXCEPTION RAISED - HAPPENED AFTER SUBMITTING TASKS AT MASTER BUT AFTER SYNC.')

    print "This message should not be printed - ERROR"


if __name__=='__main__':
    main()
