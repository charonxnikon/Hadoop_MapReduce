# Hadoop_MapReduce

Установка с виртуальной машиной
Потребуется  компилировать .jar-файлы. Мы сделали виртуальную машину с Ubuntu 22.04, на которой настроены все необходимые компоненты и среда разработки IntelliJ IDEA. Также виртуальная машина содержит пример WordCount. Запустить её можно следующим образом:
Установите на свой компьютер VirtualBox: https://www.virtualbox.org/
Загрузите виртуальную машину (7.2 ГБ): https://dione.gml-team.ru:5001/sharing/w341HTKn8
Импортируйте виртуальную машину в VirtualBox:
File → Import Appliance
Выберите загруженный файл .ova, нажмите Next
Настройки оставьте по умолчанию, нажмите Import
Нажмите Start, чтобы запустить виртуальную машину
На виртуальной машине есть папка ~/course, в которой можно найти JDK 1.7, Hadoop, IntelliJ IDEA и пример WordCount. Для сборки и запуска примера:
Запустите IntelliJ IDEA с помощью иконки на панели слева
Откройте проект WordCount
На вкладке Maven справа запустите clean и затем package:

У вас появится директория target, в ней wordcountjava-1.0-SNAPSHOT.jar — это готовый .jar-файл. В нашем случае его размер равен 5,3 КБ
Если открыть терминал в IDEA, можно запустить полученный .jar-файл следующим образом:

hadoop jar target/wordcountjava-1.0-SNAPSHOT.jar WordCount src/main/java/ output

(здесь в качестве входной директории указана папка с файлом WordCount.java за неимением лучшего текстового файла для подсчёта слов, а в качестве выходной — папка output, которая будет создана самим процессом)

В результате должен быть получен похожий файл с подсчётом «слов»:

Тот же .jar-файл теперь можно запустить на кластере, используя инструкцию ниже
Для повторного запуска hadoop jar, нужно удалить директорию output (Hadoop требует, чтобы выходной директории не было)
Для создания другой программы, можно скопировать папку wordcount, поменять artifactId в pom.xml и переименовать класс WordCount, затем перезагрузить проект в IDEA:

Установка вручную (без виртуальной машины)
Мы рекомендуем использовать виртуальную машину, так как в ней всё проверено и должно работать. Если вы всё же хотите установить компоненты вручную на своей физической машине с Linux, выполните следующие шаги:
Установите IntelliJ IDEA: https://www.jetbrains.com/idea/
Загрузите архив с JDK 1.7, Hadoop, WordCount (364 МБ): https://dione.gml-team.ru:5001/sharing/BHWAikfv4
Распакуйте его в папку, например, course
Откройте проект course/wordcount в IntelliJ IDEA
Откройте File → Project Structure, в Project → SDK выберите Add SDK → JDK…
Выберите папку course/jdk1.7.0_80. Она должна отобразиться как SDK версии 1.7
Далее можно следовать инструкции по сборке и запуску примера выше.
Если синхронизация зависимостей выдаёт ошибку, постройте проект при помощи команды package во вкладке Maven, после этого синхронизация должна пройти успешно. Либо можете попробовать добавить -Dhttps.protocols=TLSv1.2 к параметрам запуска IDEA.
Запускать проект можно через JAVA_HOME=../jdk1.7.0_80 ../hadoop-2.7.1/bin/hadoop jar target/wordcountjava-1.0-SNAPSHOT.jar WordCount src/main/java/ output
Выполнение .jar-файла на сервере Hue
Выполнение собранного файла можно производить двумя способами: локально (описано в инструкции выше) либо на сервере в Hue. На сервере есть наборы данных, используемые в заданиях, а также на сервере задания запускает автоматическая система проверки. Поэтому перед отправкой задания на проверку, вам следует убедиться, что оно запускается и работает на сервере Hue.
Инструкция по запуску на сервере Hue:

Настраиваем и проверяем VPN про прилагаемой инструкции.
Параметры подключения (логин и пароль) совпадают с параметрами подключения к кластеру и были разосланы вам отдельно.
Подключаем VPN (см. инструкцию по подключению).
Открываем общий кластер, доступный по адресу: http://users.bigdata.local:8888 
Заходим в раздел Files: 

В открывшемся разделе выбираем Upload и загружаем на кластер:
файл с расширением .jar,  
файл конфигурации config.xml,
(опционально) файл с исходными данными для тестирования.
В верхней части окна нажимаем на белый треугольник рядом с надписью Query  и выбираем редактор Java: 

Указываем необходимые параметры программы и запускаем её на счёт

Обратите внимание! 
Если в программе требуется указать путь к файлу конфигурации:
В Cloudera нужно это делать через параметр -conf, указывая его вместе с путём В ОДНОМ АРГУМЕНТЕ следующим образом:
-conf=path/to/config.xml

(см. скриншот выше).
В HUE каждый из параметров нужно передавать через опцию -D перед именами полей:

-D <имя очередного поля из файла config>=<значение этого поля>


Статистику по выполнению заданий можно посмотреть, нажав на кнопку в  правой верхней части окна:


При этом, будет выведен список всех выполненных и выполняемых задач, а также ряд  информации о них: 

Нажав на конкретную задачу, можно просмотреть ее статус, лог, а также другую  дополнительную информацию о ней:


В окне лога Query, после того как программа отработала, вы можете увидеть ошибку (на скрине ниже) и иногда не иметь возможность остановить запрос. Это нормальная ситуация, главное, чтобы  в списке задач ваша висела как выполненная и в выходной директории появились файлы.

1. CandleMapper
 получает на вход строку из csv файла и выполняет следующие действия:
Парсит строку;
Проверяет, удовлетворяет ли интрумент шаблону;
Рассчитывает время последней возможной сделки за день;
Проверяет, удовлетворяет ли момент сделки временным ограничениям, а также ограничениям по датам;
Рассчитывает момент начала свечи;
Рассчитывает номер свечи за день;
Упаковывает результат:
Key = “{инструмент},{момент начала свечи}”,
Value = “{DEAL_ID},{PRICE},{MOMENT},{номер свечи за день}”;

2. СandlePartitioner 
используется для равномерной загрузки редьюсеров. Для этого используется рассчитанный на шаге CandleMapper номер свечи. Мы раскидываем записи по редьюсерам исходя из номера свечи, взяв остаток от деления на заданное количество редьюсеров. 

Такая модель  распределения кажется наиболее оптимальной для равномерного распределения нагрузки на редьюсеры. Соседние свечи в рамках одного инструмента будут попадать на соседние редьюсеры;

3. CandleReducer 
получает на вход информацию о сделках в рамках одной свечи, проходит по всем значениям Value  и выполняет следующее:
Парсит строку и приводит данные из строкового типа в тип, соответствующий переменной;
Рассчитывает цену первой сделки (с учетом дублирующихся моментов);
Рассчитывает цену последней сделки (с учетом дублирующихся моментов);
Рассчитывает максимальные и минимальные цены;
Упаковывает результат:
Key = “{инструмент},{момент начала свечи}”,
Value = “{OPEN},{HIGH},{LOW}{CLOSE}”;
Результат пишется в файл {инструмент}{номер редьюсера}
Результаты local:
Шаг 0: скачать данные с кластера и запустить программу на локальном компьютере с указанием ширины свечи в 1000 мс и остальными стандартными параметрами, за исключением редьюсеров. 

В качестве сетки для тестирования выбран следующий набор количества редьюсеров: 1,2,4,8,16,32,64,128,256. Результаты работы приведены на графике


Оптимальным числом редьюсеров оказалось 4.

Характеристики локальной машины: 
 
Результат сильно зависит от характеристик машины, почти наверное оптимальное число при запуске на кластере окажется другим
Результаты кластер:
Для протокола: подключение к кластеру


Оптимальным числом редьюсеров оказалось 16.
Запуск и исходный код

$HADOOP_HOME/bin/hadoop jar target/candle.jar candle -conf config.xml input output

