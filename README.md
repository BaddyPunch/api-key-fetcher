Обработка данных по делам

Этот проект предоставляет решение для получения и фильтрации данных по делам из API Casebook с использованием Apache Spark. 
Он извлекает список идентификаторов дел из CSV-файла, выполняет параллельные API-запросы для сбора соответствующих данных и сохраняет отфильтрованные результаты в новый CSV-файл.

Возможности
- Получение API-ключа и его кэширование для повторного использования, чтобы минимизировать количество API-вызовов.
- Чтение идентификаторов дел из CSV-файла.
- Выполнение параллельных API-запросов для извлечения данных по делам.
- Фильтрация данных на основе указанной даты.
- Вывод отфильтрованных результатов в новый CSV-файл.
