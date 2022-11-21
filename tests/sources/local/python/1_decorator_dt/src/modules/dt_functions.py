from pycompss.api.task import task


@task(returns=object)
def bb(A):
    A.append("from_bb")
    return A


@task(returns=object)
def aa(A):
    A.append("from_aa")
    return A


def app_task(A):
    return aa(bb(A))


def appender(A):
    A.append("from_dt")
    return A


def appender_w_param(a_list, tba):
    a_list.append(tba)
    return a_list


def ctf (col, data_filename):
    with open (data_filename, "w") as f:
        f.writelines(col)
        f.write(" serialized")


def otf(obj, fayl):
    with open(fayl, "w") as fo:
        fo.write(obj)
        fo.write(" serialized")
    print(fayl)


def fto(fayl):
    with open(fayl, "r") as nm:
        ret = nm.read()
    return ret


def ftc(fayl):
    with open(fayl, "r") as nm:
        ret = nm.read()
    ret = ret.split()
    return ret


def cto(data):
    return " ".join(data)

