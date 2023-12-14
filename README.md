Это мой учебный проект хранилища данных в PostgreSQL. Идея в том, что из [источника](https://drive.google.com/drive/folders/12gPWRWku1FBQrulvPyrthSajBrL2CE6E?usp=sharing) (csv файлы) данные о продажах в сети магазинов проходят через несколько ступеней обработки и попадают в таблицы в слое 3nf и в слое dm. При изменении csv файлов и повторном запуске в таблицах на последних слоях появятся только новые данные, а старые, если в них были изменения, будут редактированы.

[Схема слоя 3nf.](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/3nf_scheme.png)

[Схема слоя dm.](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/dm_scheme.png)

[Схема движения данных для таблицы фактов.](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/data_flow.png)

## Описание файлов с кодом:

[dataset_creation.ipynb](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/dataset_creation.ipynb)  - это код на Python, который берёт первичный датасет, скаченный с kaggle и приводит его к подходящему к требованиям проекта состоянию. В основном там удаляются ненужные колонки и добавляются новые. Используется pandas, numpy и библиотека Faker для генерации данных. Он сделан в Jupyter Notebook, но его можно просматривать в PyCharm. 

[DEMO_DWH_1_DDL_DCL](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_1_DDL_DCL.sql) - DDL скрипт, создающий слои и таблицы в них. Через foreign data wrapper подключаемся к csv файлам. Их два - cash sales и card sales. В требованиях было два отличающихся источника данных. Создаются схемы bl_cl (слой для разработчиков), sa (staging area - предварительная обработка данных), bl_3nf - нормализованный слой в котором хранятся конечные данные, dl_dm - денормализованный слой с конечными данными. Также предусмотрено партиционирование фактовых таблиц по месяцам.

[DEMO_DWH_2_Deduplication](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_2_Deduplication.sql) - скрипт, в котором с помощью процедуры производится дедупликация данных для одной из таблиц.

[DEMO_DWH_3_Functions](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_3_Functions.sql) - процедура logger, которая потом вызывается при каждой операции с загрузкой строк и записывает данные в таблицу логов и функции для трасфера данных из external таблицы в статичную, с которой в дальнейшем будет идти работа.

[DEMO_DWH_4_DEMO_DWH_4_Loading_functions_to_3nf](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_4_Loading_functions_to_3nf.sql) - процедуры последовательной загрузки данных в нормализованный слой bl_3nf.

[DEMO_DWH_5_Loading_functions_to_dm](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_5_Loading_functions_to_dm.sql) - процедуры загрузки данных из слоя 3nf в bl_dm и большая процедура run_etl, вызов которой запускает весь пайплайн.

[DEMO_DWH_6_SCD_handling](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_6_SCD_handling.sql) - триггерные функции, осуществляющие slow changing dimension type 2 для таблицы клиентов. Смысл в том, что если в новой порции данных данные о клиенте изменились, то в слоях 3nf появятся новые строки с актуальной информацией и датой обновления, а старая строка пометится как устаревшая, также с датой устаревания. Вот [картинка](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/scd2_example.png) из учебника по dwh для наглядности.

[DEMO_DWH_7_Data_load](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_7_Data_load.sql) - скрипт, из которого запускается загрузочная процедура и в котором удобно смотреть на текущее состоянии таблиц.

[DEMO_DWH_8_Tests](https://github.com/spike0spiegel/dwh_demo_meniailov/blob/main/DEMO_DWH_8_Tests.sql) - несколько функции для диагностики и тестирования. 




