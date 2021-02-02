

def fun1(fun):
    fun()
    print('end')

@fun1()
def fun2():
    print('start')
    return fun2

fun2()
