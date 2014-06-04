process
=======

MS OLAP

*****************************UA*********************************************
* Ідею і власне програму, на основі якої ця програма розвинулась - була любязно надана Ігором Бобаком. За що йому велика подяка.
*
* Призначення програми - автоматичний процесінг кубів. + автоматичне створення партицій по потребі.
* Процесінг кубів може бути розбитий на кроки. Програма вміє очікувати завершення розрахунку даних в SQL
* А також вміє контролювати допустимий період процесінгу наприклад щоб робочий час не процесилось.
* Типічний рядок запуску в job: D:\Process\process.exe /DB:DW_OLAP3 /Server:localhost /step:10
* Запуск для процесінга куба, наприклад після змін з командного рядка: process.exe /db:dw_olap3 /cube:чеки_товар /parallel:2
* Програма вміє перезапускати службу MSSQLServerOLAPService а при старті крока перевіряє чи служба запущена і якщо ні пробує її запустити.
* Права на це повинні бути у користувача від чийого імені вона стартує.
* Групи мір, яку необхідно процесити в даному кроці визначається наступним чином. 
* в полі description групи мір має бути рядок виду pr=>31,10; pr=> - признак що це інфа про крок і період перепроцесінга. крок в даному випадку 31, 10 -скільки днів перепроцешувати партіцию за попередній період.
* Якщо  description незаповнений або не починається з pr=>  то крок по замовчуванню для цьої групи мір - 11.
* Партіциї створються на основі партіциї з  назвами  template,template_Month,template_Quarter,template_Year,template_4Week,template_Week  
* Партіциї template - це помісячні партіциї в них запит має відповідати шаблону 
* 	SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
* 	Де 20130601 - початковий період. Доступо створення партіций як для Oracle так MSSQL.
* 	Для MSSQL запит виду  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>='20130601' 
* Для партіций з назвами template_Month,template_Quarter,template_Year,template_4Week,template_Week
* дещо інший спосіб формування запитів. Вони допускають довільної складності запит.
*  Логіка формування наступна: to_date('20130601','YYYYMMDD') -вказує на початок періоду і заміняється на реальний початок періоду а 
*  to_date('00010101','YYYYMMDD') заміняється на кінець періоду
*  Доступо створення партіций тільки для Oracle
* Для шаблонів template_4Week,template_Week дата початку періоду може бути будь-який день тижня - визначається початковою датою.
* 
* Програма має конфігураційний файл process.xml Параметри з рядка мають перевагу над конфігураційним файлом.
* Типовий конфігураційний файл у нас. Достатньо очевидні параметри.

<Config>
 <Server>srv-bat</Server>
 <Database>DW_OLAP</Database>
 <XMLA maxParallel="6">true</XMLA>
 <DefaultStep>0</DefaultStep>
 <ConectSQL>Provider=OraOLEDB.Oracle.1;Data Source=mer;Persist Security Info=True;user id=*;password=*</ConectSQL>
 <Metod>Fast</Metod> <!-- default:Fast (Fact,Full,Normal)  -->
 <KeyErrorLogPath>d:\process\log</KeyErrorLogPath> <!-- delault - program_path\LOG ; path -server\path -->
 <ServicesOlap>MSSQLServerOLAPService</ServicesOlap> 
 <Step0>
  <Time Start="7" End="24">true</Time>
  <ProcessDimension>UPDATE</ProcessDimension> <!-- default:UPDATE (UPDATE,FULL) --> 
  <RestartServicesOlap>1</RestartServicesOlap> <!-- default:0 - no restart,1 before,2-after,3 before and after  -->
 </Step0>

 <Step1>
  <PrepareSQL> begin null; end;  </PrepareSQL> <!-- planed  -->
 </Step1>

 <Step2>
   <WaitSQL>SELECT * FROM dw.v_statecons</WaitSQL> 
 </Step2>
 
 <Step3>
   <WaitSQL>SELECT * FROM dw.v_state_MIN_MAX</WaitSQL> 
 </Step3>

</Config>
 
 
*/
