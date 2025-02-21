'''
Переменные. Типы данных (int, float, str, list), операции над ними. Функция print
''' 

a = 5                   # int
b = 5.5                 # float
c = 'Программирование'  # str
d = [1, 2, 3, 4, 5]     # list

# --------------

a = 5
b = 10
sum_ab = a + b
print("Сумма:", sum_ab)

# --------------

x = 5.5
y = 2.0
product_xy = x * y
print("Произведение:", product_xy)

a = 567
b = 23
c = a/b

print('Частное', c)

# --------------

first_name = "Иван"
last_name = "Иванов"
full_name = first_name + " " + last_name
print("Полное имя:", full_name)

# --------------

fruits = ["яблоко", "банан", "вишня"]
print("Фрукты:", fruits)
fruits.append("апельсин")
print("Фрукты после добавления:", fruits)

# --------------







from math import *

# -------------- Data types & Variables -------------
print('--------- Data types & Variables ---------\n')

# Most helpful
Int = 10
Float = 9.99
Str = 'Ninety nine'
List = ['One', 'Two', 'Three']
Dict = {'One':1, 'Two':2, 'Three':3}
Bool = True

# Others
Tuple = ('One', 'Two', 'Three')
Set = {'One', 'Two', 'Three'}
Bytes = b'Hello World!'
Bytearray = bytearray(10)
Memoryview = memoryview(bytes(10))

print(
    Int, type(Int),'\n',
    Float, type(Float),'\n',
    Str, type(Str),'\n',
    List, type(List),'\n',
    Tuple, type(Tuple),'\n',
    Set, type(Set),'\n',
    Dict, type(Dict),'\n',
    Bool, type(Bool),'\n',
    Bytes, type(Bytes),'\n',
    Bytearray, type(Bytearray),'\n',
    Memoryview, type(Memoryview),'\n'
    )

# Revirse variables
x = 5
y = 34
print(x, y)
x, y = y, x
print(x, y)

# -------------- Operations with lists -------------
print('--------- Operations with lists ---------\n')

print(List[1]) # Two
print(List[1:]) # 'Two', 'Three']
print(List[0:2]) # ['One', 'Two']
List[2] = 3 # ['One', 'Two', 3]
print(List)
List.append(4)
print(List) # ['One', 'Two', 3, 4]
List2 = [7, 5, 6]
List.extend(List2)
print(List) # ['One', 'Two', 3, 4, 5, 6, 7]
List.insert(0, 0)
print(List) # [0, 'One', 'Two', 3, 4, 5, 6, 7]
List.remove(7)
print(List) # [0, 'One', 'Two', 3, 4, 5, 6]
List.clear()
print(List) # []
print(List2.index(6)) # 2
List2.sort()
print(List2) # [5, 6, 7]
List3 = List2.copy()
print(List3) # [5, 6, 7]

# -------------- Operations with strings -------------
print('--------- Operations with strings ---------\n')
a = 'Hello'
b = 'World'
c = '!'
print(a, b, c)
print(a + '_' + b + '_' + c)
print(a + ' World !')
print('\n',a,'\n', b, '\n', c, '\n')
w = 'WORD'
print(w.lower()) # word
print(w.upper()) # WORD
print(w.isupper()) # True
print(len(w)) # 4
print(w[2]) # R
print(w.index('D')) # 3
print('Hello World !'.replace('World', 'User')) # Hello User !

# -------------- Operations with numbers -------------
print('--------- Operations with numbers ---------\n')
a = 3
b = 4
print(a + b)
print(a - b)
print(a * b)
print(a / b)
print(a // b)
print(a % b)
print(a ** b)
print(-a)
print((a * (b + (a % b)))/ (b / a))

print(str(a + b) + ' is string') # int to str
print(pow(a, 2)) # a^2
print(max(a, b))
print(min(a, b))
print(round(3.90746597))
print(sqrt(15625))

# -------------- Getting Input -------------
print('--------- Getting Input ----------\n')

name = input('Hello! Enter your name: ')
print('Hello, ' + name + '!')
age = input('Now let`s find out your age: ')

# -------------- if, elif, else -------------
print('--------- if, elif, else ----------\n')

if 0 < int(age) < 10:
    print('It`s nice to enjoy your childhood at the age of', age)
elif 11 < int(age) < 19:
    print('Oh, that carefree teenage life at the age of', age)
elif 20 < int(age) < 35:
    print('Simply', age, 'is the best age!')
elif 36 < int(age) < 50:
    print('Probably you are family person at the age of', age)
elif 51 < int(age) < 70:
    print('I bet you are chilling right now at the age of', age)
else:
    print(age, 'is nice retirement age')



