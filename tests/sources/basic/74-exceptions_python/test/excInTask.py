from pycompss.api.task import task

@task(returns=int)
def increment(v):
    raise Exception('GENERAL EXCEPTION RAISED - HAPPENED IN A TASK.')
    return v+1

def main():
    from pycompss.api.api import compss_wait_on
    value = 0
    value = increment(value)
    result = compss_wait_on(value)

    if result == 1:
        print "- Result value: OK"
    else:
        print "- Result value: ERROR"
        print "- This error is a root error. Please fix error at test 19."


if __name__=='__main__':
    main()
