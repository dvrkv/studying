'''
1 Необходимо написать программу, которая принимает целое число и возвращает это же число с цифрами в обратном порядке

2 Напишите функцию, которая принимает список чисел и возвращает их сумму.

3 Напишите функцию, которая проверяет, является ли строка палиндромом

4 Напишите функцию, которая находит максимальное число в списке без использования встроенной функции max()

5 Напишите функцию, которая принимает список чисел и возвращает новый список, содержащий только четные числа из исходного списка.

6 Напишите функцию, которая принимает список и возвращает новый список без дубликатов
'''

def reverser(x):
    x = str(x)[::-1]
    return x

def summer(x):
    sum = 0
    for i in x:
        sum = sum + i
    return sum

def isPoli(x):
    if x.lower() == x.lower()[::-1]:
        print('Это полиндром !')
    else:
        print('Это НЕ полиндром !')

def isMax(x):
    max = 0
    for i in x:
        if i > max:
            max = i
    return max

def evenList(x):
    evenList = []
    for i in x:
        if i % 2 == 0:
            evenList.append(i)
    return evenList

def unique(x):
    return list(set(x))

# print(reverser(2367))
# print(summer([1, 2, 3]))
# print(isPoli('Топот'))
# print(isMax([1, 2, 3]))
# print(evenList([1, 2, 3, 4]))
# print(unique([1, 1, 2, 2, 3, 3]))





