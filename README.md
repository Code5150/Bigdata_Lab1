# Лабораторная работа № 1: Введение в Apache Spark
***

## Цель работы:
* изучить операции загрузки и выгрузки данных в HDFS,
* ознакомиться с базовыми операциями Apache Spark в spark-shell,
* создать проект по обработке данных в IDE,
* отладить анализ данных велопарковок на локальном компьютере,
* запустить анализ данных велопарковок на сервере.


## Анализ данных велопарковок

1. Найти велосипед с максимальным временем пробега

Сначала данные о поездках преобразуются в пару (id велосипеда, длительность).
Далее пары складываются по ключу, а затем выбирается пара с максимальной длительностью.

```
    val bikeWithMaxRun = tripsInternal.map(x => (x.bikeId, x.duration)).reduceByKey(_ + _)
      .max()(Ordering.by[(Int, Int), Int](_._2))
```

2. Найти наибольшее расстояние между станциями

Производим декартово произведение таблицы stationsInternal на саму себя при помощи ```cartesian```. Для каждой пары станции составляем кортеж: пара id станций и расстояние между ними. После этого выбирается кортеж с наибольшим расстоянием.

```
    val distancesBetweenStations = sc.broadcast(stationsInternal.cartesian(stationsInternal).map(sp =>
      ((sp._1.stationId, sp._2.stationId), distanceBetween(sp._1, sp._2))
    ).collect().toMap)
    val biggestDistanceBetweenStations = distancesBetweenStations.value.max(Ordering.by[((Int, Int), Double), Double](_._2))
```

3. Найти путь велосипеда с максимальным временем пробега через станции

Сперва производится фильтрация записей, где отбрасываются все пути, которые не посетил велосипед с наибольшим пробегом.
Затем сортируем записи по дате отправления.

```
  val bikeWithMaxRunPath = tripsInternal.filter(t => t.bikeId == bikeWithMaxRun._1)
      .sortBy(_.startDate.toInstant(ZoneOffset.UTC).toEpochMilli)
```

4. Найти количество велосипедов в системе

Выбираем из информации о поездках id велосипедов. Количество велосипедов можно посчитать при помощи ```count()```. Также для исключения повторов надо поставить условие недопуска дубликатов ```distinct()```.

```
  val bikes = tripsInternal.map(trip => trip.bikeId).distinct().count()
```

5. Найти пользователей, потративших на поездки более 3 часов

Производится фильтрация по длительности поездки среди велосипедов, у которых длительность пробега больше 3 часов.

```
  val bikesWithMoreThan3Hours = tripsInternal.keyBy(rec => rec.bikeId)
    .mapValues(x => x.duration)
    .reduceByKey(_ + _)
    .filter(p => p._2 > 10800)
```

Результат выполнения программы:

![Вывод программы](https://github.com/Code5150/Bigdata_Lab1/blob/master/img/results.jpg)

## Запуск в кластере

Проверим, что выдача нашей программы на сервере идентична выдаче в IDE при запуске на локальном компьютере. Запустим скомпилированный jar файл с помощью команды /opt/mapr/spark/spark-2.3.1/bin/spark-submit --deploy-mode cluster --master yarn spark-lab_2.11-0.1.jar yarn.

Логи приложения, запущенного на сервере, доступны в папке /opt/mapr/hadoop/hadoop-2.7.0/logs/userlogs/<app_id>.

![Вывод программы в кластере](https://github.com/Code5150/Bigdata_Lab1/blob/master/img/results_cluster.jpg)

Можно увидеть, что выдачи программ идентичны с точностью до записей об ошибках (которые при запуске на сервере записываются в отдельный файл).

***

## Заключение

В ходе выполнения лабораторной работы были изучены основные возможности экосистемы Spark, успешно выполнен анализ данных о велосипедных парковках и на локальном компьютере, и на сервере.