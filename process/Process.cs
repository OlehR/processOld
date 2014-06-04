/*
*****************************UA*********************************************
* Ідею і власне програму, на основі якої ця програма розвинулась - була любязно надана Ігором Бобаком
* з компанії BIT Impulse (www.bitimpulse.com) - архітектором BI-системи Business Analysis Tool. 
* За що йому велика подяка.
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
* Програма має конфігураційний файл process.xml Параметри з рядка запуску мають перевагу над конфігураційним файлом.
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
  <RestartServicesOlap>1</RestartServicesOlap> <!-- default:0 - no restart,1 - before,2-after,3 before and after  -->
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
 
* 
*/
using System;
using System.Data;
using System.Threading;
using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.Xmla;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Globalization;
using System.Data.OleDb;
using System.Xml;
using System.ServiceProcess;
//using System.Diagnostics;

//using System.Xml.XPath;
//using System.Threading.Tasks;

namespace Process
{
	class GlobalVar
	{
		public static string varServer="localhost",varDB="dw_olap",varCube=null;
		public static string  varPrepareSQL=null,varWaitSQL=null,varConectSQL=null;
		public static ProcessType varProcessDimension = ProcessType.ProcessUpdate, varProcessCube = ProcessType.ProcessFull ;
		public static bool varIsProcessDimension = false, varIsProcessCube = true;
		public static string varFileLog=null;
		public static string varFileXML=null;
		public static string varKeyErrorLogFile=Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)+ "\\log\\Error_"+DateTime.Now.ToString("yyyyMMdd")+".log";
		public static int varStep=999, varMetod=0;
		public static int varTimeStart=0,varTimeEnd=24 ;
		public static int varMaxParallel=0;
		public static int varDayProcess=20;
		public static DateTime varDateStartProcess= DateTime.Now;
		public static bool varIsArx= false;
		public static DateTime varArxDate= new DateTime (1,1,1);
		public static string varServicesOlap ="";
		public static int varRestartServicesOlap =0;
	}
	
	class Program
	{
		public static void Main(string[] args)
		{
			string varFileXML=Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)+"\\process.xml";
			Services varService;
			string varKey = @"
Доступнi ключi: /XML:process.xml /Server:localhost /DB:dw_olap /Cube: /Step:0 /PARALLEL:8 /Metod:0
/XMLA: 1 - процесити через XMLA, 0 - не використовувати XMLA для не INDEX процесінга
/Metod:0 - ігнорувати помилки ключі з конвертацією, 1 - пробувувати без ігнорування
/DAY: - за скільки днів перепроцешувати куби  за замовчуванням 20
/STATE (:2) - Показує стани кубів. :2-Розширена інформація
/DATESTART:DD.MM.YYYY - З Якої дати процесити партіциї.
/PROCESSDIMENSION: - (NONE,UPDATE,FULL)  По замовчуванню UPDATE
/PROCESSCUBE: - (NONE,DATA,FULL) По замовчуванню FULL
/ARX:01.01.2012 - Процесити в режимі архів. (З якої дати розширювати партиції) дата не обов'язковий параметр.

";
			
			//Перевіряємо чи є в параметрах XML файл
			for (int i=0;i<args.Length;i++)
				if (args[i].ToUpper().StartsWith("/XML:"))
					varFileXML=args[i].Substring(5);
			else if (args[i].ToUpper().StartsWith("/STEP:"))
				GlobalVar.varStep=Convert.ToInt32( args[i].Substring(6));
			
			//Перевіряємо наявність XML файла
			if(File.Exists(varFileXML))
			{
				MyXML myXML =new MyXML(varFileXML);
				GlobalVar.varServer= ( myXML.GetVar("Server") == null? GlobalVar.varServer : myXML.GetVar("Server") );
				GlobalVar.varDB = ( myXML.GetVar("Database") == null? GlobalVar.varDB : myXML.GetVar("Database") );
				GlobalVar.varServicesOlap = ( myXML.GetVar("ServicesOlap") == null? GlobalVar.varDB : myXML.GetVar("ServicesOlap") );
				//GlobalVar.varStep = ( myXML.GetVar("DefaultStep") == null? GlobalVar.varStep : Convert.ToInt32( myXML.GetVar("DefaultStep")) );
				GlobalVar.varMaxParallel = (myXML.GetAttribute("maxParallel","XMLA")==null ?  GlobalVar.varMaxParallel : Convert.ToInt32(myXML.GetAttribute("maxParallel","XMLA") )) ;
				GlobalVar.varConectSQL = myXML.GetVar("ConectSQL");
				GlobalVar.varKeyErrorLogFile = (myXML.GetVar("KeyErrorLogPath")==null ? GlobalVar.varKeyErrorLogFile :myXML.GetVar("KeyErrorLogPath")+"\\Error_"+DateTime.Now.ToString("yyyyMMdd")+".log");
				if(myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"ProcessDimension")!=null )
					MyXMLA.SetProcessTypeDimension( myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"ProcessDimension"));
				GlobalVar.varPrepareSQL  = myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"PrepareSQL");
				GlobalVar.varWaitSQL     = myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"WaitSQL");
				GlobalVar.varTimeStart = ( myXML.GetAttribute("Start","Step" +GlobalVar.varStep.ToString().Trim(),"Time")==null ? GlobalVar.varTimeStart : Convert.ToInt32(myXML.GetAttribute("Start","Step" +GlobalVar.varStep.ToString().Trim(),"Time") ));
				GlobalVar.varTimeEnd = ( myXML.GetAttribute("End","Step" +GlobalVar.varStep.ToString().Trim(),"Time")==null ? GlobalVar.varTimeEnd : Convert.ToInt32(myXML.GetAttribute("End","Step" +GlobalVar.varStep.ToString().Trim(),"Time") ));
				GlobalVar.varRestartServicesOlap   = (myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"RestartServicesOlap")==null? GlobalVar.varRestartServicesOlap : Convert.ToInt32(myXML.GetVar("Step" +GlobalVar.varStep.ToString().Trim(),"RestartServicesOlap")));
			}
			// 			string var= GlobalVar.varProcessDimension;
			
			//Параметри з командного рядка мають перевагу.
			for (int i=0;i<args.Length;i++)
			{
				if (args[i].ToUpper().StartsWith("/SERVER:"))
					GlobalVar.varServer=args[i].Substring(8);
				else if (args[i].ToUpper().StartsWith("/DB:"))
					GlobalVar.varDB=args[i].Substring(4);
				else if (args[i].ToUpper().StartsWith("/CUBE:"))
					GlobalVar.varCube=args[i].Substring(6);
				else if (args[i].ToUpper().StartsWith("/STEP:"))
					GlobalVar.varStep=Convert.ToInt32( args[i].Substring(6));
				else if (args[i].ToUpper().StartsWith("/PARALLEL:"))
					GlobalVar.varMaxParallel =Convert.ToInt32( args[i].Substring(10));
				else if (args[i].ToUpper().StartsWith("/XML:"))
					GlobalVar.varFileXML=args[i].Substring(5);
				else if (args[i].ToUpper().StartsWith("/DAY:"))
					GlobalVar.varDayProcess=Convert.ToInt32(args[i].Substring(5));
				else if (args[i].ToUpper().StartsWith("/STATE:2"))
					GlobalVar.varStep=-9998;
				else if (args[i].ToUpper().StartsWith("/STATE"))
					GlobalVar.varStep=-9999;
				else if (args[i].ToUpper().StartsWith("/DATESTART:"))
					GlobalVar.varDateStartProcess =DateTime.Parse(args[i].Substring(11));
				else if (args[i].ToUpper().StartsWith("/PROCESSDIMENSION:"))
					MyXMLA.SetProcessTypeDimension (args[i].Substring(17));
				else if (args[i].ToUpper().StartsWith("/PROCESSCUBE:"))
					MyXMLA.SetProcessTypeCube( args[i].Substring(13));
				else if (args[i].ToUpper().StartsWith("/ARX"))
				{
					GlobalVar.varIsArx=true;
					if(args[i].ToUpper().Length==14)
						GlobalVar.varArxDate = DateTime.ParseExact(args[i].ToUpper().Substring(5),"dd.MM.yyyy",CultureInfo.InvariantCulture);
				}
				
				else if (args[i].ToUpper().StartsWith("/?"))
				{
					Console.Write(varKey);
					Console.ReadKey(true);
					return;
				}
				else
				{
					Console.Write("Колюч=>"+ args[i].ToUpper() + " невірний. "+varKey);
					Console.ReadKey(true);
					return;
				}
			}
			
			
			GlobalVar.varFileLog = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)+
				"\\log\\process_"+GlobalVar.varDB.Trim()+"_"+
				DateTime.Now.ToString("yyyyMMdd")+"_"+ GlobalVar.varStep.ToString().Trim() +".txt";

			Log.log("START=> /Server:"+GlobalVar.varServer+" /DB:"+GlobalVar.varDB+" /CUBE:"+GlobalVar.varCube+" /Step: "+GlobalVar.varStep.ToString()   );
			if(GlobalVar.varServicesOlap.Trim().Length>0)
			{
				varService = new Services(GlobalVar.varServicesOlap, GlobalVar.varServer);
				if(!varService.IsStart() || GlobalVar.varRestartServicesOlap==1 || GlobalVar.varRestartServicesOlap==3)
				{
					if(varService.IsStart())
					{
						Log.log("Try ReStart =>"+ GlobalVar.varServicesOlap +" in " + GlobalVar.varServer);
						if(!varService.ReStart())
						{
							Log.log("No ReStart =>"+ GlobalVar.varServicesOlap +" in " + GlobalVar.varServer+"\n"+varService.log);
							return;
						}
						else
							Log.log("ReStart OK");
					}
					else
					{
						if(!varService.Start())
						{
							Log.log("Try start =>"+ GlobalVar.varServicesOlap +" in " +GlobalVar.varServer);
							if(!varService.Start())
							{
								Log.log("No start =>"+ GlobalVar.varServicesOlap +" in " + GlobalVar.varServer+"\n"+varService.log);
								return;
							}
							else
								Log.log("Start OK");
						}
					}
				}
			}
			XMLABuilder.Process(@"Data Source="+GlobalVar.varServer+";Provider=msolap;", GlobalVar.varDB,GlobalVar.varCube,GlobalVar.varStep,GlobalVar.varMetod);
			Log.log("END=> /Server:"+GlobalVar.varServer+" /DB:"+GlobalVar.varDB+" /CUBE:"+GlobalVar.varCube+" /Step:"+GlobalVar.varStep.ToString()   );
			if(GlobalVar.varStep<-9990)
				Console.ReadKey(true);
		}
		
	}

/*
	internal enum ObjectType
	{
		otPartition,
		otMeasugeGroup,
		otDimension
	}*/
	
	/// <summary>
	/// Клас для збереження Log.
	/// </summary>
	public static class Log
	{
		public static void  StrToFile(string cFileName, string cExpression)
		{
			StrToFile(cFileName, cExpression, FileMode.CreateNew);
		}

		public static void log(string cExpression,string parFile = null)
		{
			if (parFile == null)
				if(GlobalVar.varFileLog==null)
					parFile=Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)+
						"\\log\\process_"+DateTime.Now.ToString("yyyyMMdd")+".txt";
				else
					parFile=GlobalVar.varFileLog;
			try
			{
				DateTime now = DateTime.Now;
				StreamWriter sw;
				FileInfo fi = new FileInfo(parFile);
				sw = fi.AppendText();
				sw.WriteLine(now.ToString() + "=>" + cExpression);
				sw.Flush();
				sw.Close();
				Console.WriteLine(now.ToString() + "=>" + cExpression);
				
				//             StrToFile(Environment.GetEnvironmentVariable("temp") + "\\log_process.txt", "\n" + now.ToString() + "=>" + cExpression, FileMode.OpenOrCreate);
			}
			catch
			{
			};

		}

		static public void StrToFile(string cFileName, string cExpression, System.IO.FileMode parFileMode)
		{
			if ((System.IO.File.Exists(cFileName) == true) && (System.IO.FileMode.OpenOrCreate != parFileMode))
			{
				//If so then Erase the file first as in this case we are overwriting
				System.IO.File.Delete(cFileName);
			}

			//Create the file if it does not exist and open it
			FileStream oFs = new FileStream(cFileName, parFileMode, FileAccess.ReadWrite);
			
			//Create a writer for the file
			StreamWriter oWriter = new StreamWriter(oFs);

			//Write the contents
			oWriter.Write(cExpression);
			oWriter.Flush();
			oWriter.Close();
			oFs.Close();
		}
	}

	

	class ProcessingOnLine
	{
		Partition Par = null;
		Dimension Dim = null;
		MeasureGroup Meas = null;

		ErrorConfiguration ErrConf;

		public ProcessType ProcessingType;
		public ProcessingOnLine(ProcessType parProcessingType, Dimension parDimension)
		{
			ProcessingType = parProcessingType;
			Dim = parDimension;
		}
		public ProcessingOnLine(ProcessType parProcessingType, Partition parPartition)
		{
			ProcessingType = parProcessingType;
			Par = parPartition;
		}
		public ProcessingOnLine(ProcessType parProcessingType, MeasureGroup parMeas)
		{
			ProcessingType = parProcessingType;
			Meas = parMeas;
		}
		
		public void Process(int  parMetod = -1)
		{
			
			if(parMetod == -1)
				parMetod=GlobalVar.varMetod;
			//Log log =new Log();
			ErrConf = new ErrorConfiguration();
			ErrConf.KeyErrorAction = KeyErrorAction.ConvertToUnknown;
			ErrConf.KeyNotFound = ErrorOption.IgnoreError;
			ErrConf.NullKeyConvertedToUnknown = ErrorOption.IgnoreError;
			if (Dim != null)
				try
			{
				Log.log("ProcesDimension=>" + Dim.Name);
				Dim.Process(ProcessingType);
			}
			catch (Exception e)
			{
				if (ProcessType.ProcessFull != ProcessingType)
					try
				{
					Log.log("try ProcessADD =>" + Dim.Name +"\n"+ e.Message);
					Dim.Process(ProcessType.ProcessAdd);
				}
				catch (Exception e2)
				{
					Log.log("Error ProcessFull =>" + Dim.Name+"\n"+ e2.Message);
				}
			}
			finally
			{
				Log.log("End ProcesDimension=>" + Dim.Name);
			};

			if (parMetod == 0)
			{
				
				if (Par != null)
					try
				{
					Log.log("ProcesPartition (ConvertToUnknown) =>" + Par.ParentCube.Name + "." + Par.Parent.Name+"." +Par.Name);
					Par.Process(ProcessingType,ErrConf);
				}
				catch(Exception e)
				{
					if(ProcessingType!=ProcessType.ProcessFull)
					{
						try
						{
							Log.log("try ConvertToUnknown + ProcessFull =>" + Par.Name+"\n"+e.Message);
							Par.Process(ProcessType.ProcessFull, ErrConf);
						}
						catch(Exception e2)
						{
							Log.log("Error ConvertToUnknown =>" + Par.Name+"\n"+e2.Message);
						}
					}
				}
				
				finally
				{
					Log.log("End ProcesPartition=>" + Par.ParentCube.Name + "." + Par.Parent.Name+"."+ Par.Name );
				};
				if (Meas != null)
					try
				{
					Log.log("ProcesPartition (ConvertToUnknown) =>" + Meas.Parent.Name + "." + Meas.Name);
					Meas.Process(ProcessingType,ErrConf);
				}
				catch(Exception e)
				{
					if(ProcessingType!=ProcessType.ProcessFull)
					{
						
						try
						{
							Log.log("try ConvertToUnknown + ProcessFull =>" + Meas.Name+"\n"+e.Message);
							Meas.Process(ProcessType.ProcessFull, ErrConf);
						}
						catch(Exception e2)
						{
							Log.log("Error ConvertToUnknown =>" + Meas.Name+"\n"+e2.Message);
						}
					}
				}

				finally
				{
					Log.log("End ProcesPartition=>" + Meas.Parent.Name + "." + Meas.Name);
				};

			}
			else
			{

				if (Par != null)
					try
				{
					Log.log("ProcesPartition=>" + Par.ParentCube.Name + "." + Par.Parent.Name+"." +Par.Name );
					Par.Process(ProcessingType);
				}
				catch(Exception e)
				{
					try
					{
						Log.log("try ConvertToUnknown + ProcessFull =>" + Par.Name+"\n"+e.Message);
						Par.Process(ProcessType.ProcessFull, ErrConf);
					}
					catch(Exception e2)
					{
						Log.log("Error ConvertToUnknown =>" + Par.Name+"\n"+e2.Message);
					}
				}
				
				finally
				{
					Log.log("End ProcesPartition=>" + Par.ParentCube.Name + "." + Par.Parent.Name+"."+ Par.Name );
				};
				if (Meas != null)
					try
				{
					Log.log("ProcesPartition=>" + Meas.Parent.Name + "." + Meas.Name);
					Meas.Process(ProcessingType);
				}
				catch(Exception e)
				{
					try
					{
						Log.log("try ConvertToUnknown + ProcessFull =>" + Meas.Name+"\n"+e.Message);
						Meas.Process(ProcessType.ProcessFull, ErrConf);
					}
					catch(Exception e2)
					{
						Log.log("Error ConvertToUnknown =>" + Meas.Name+"\n"+e2.Message);
					}
				}

				finally
				{
					Log.log("End ProcesPartition=>" + Meas.Parent.Name + "." + Meas.Name);
				};
				
			}
		}

		public string GetXMLA()
		{
			string process = null;
			
			if (ProcessingType == ProcessType.ProcessAdd)
				process = "ProcessAdd";
			else if (ProcessingType == ProcessType.ProcessFull)
				process = "ProcessFull";
			else if (ProcessingType == ProcessType.ProcessIndexes)
				process = "ProcessIndexes";
			else if (ProcessingType == ProcessType.ProcessUpdate)
				process = "ProcessUpdate";
			else if (ProcessingType == ProcessType.ProcessData)
				process = "ProcessData";

			if (Par != null)
				return
					@"   <Process>
      <Object>
        <DatabaseID>" + Par.ParentDatabase.ID + @"</DatabaseID>
        <CubeID>" + Par.ParentCube.ID + @"</CubeID>
        <MeasureGroupID>" + Par.Parent.ID + @"</MeasureGroupID>
        <PartitionID>" + Par.ID + @"</PartitionID>
      </Object>
      <Type>" + process + @"</Type>
     </Process>";

			if (Meas != null)
				return
					@"   <Process>
      <Object>
        <DatabaseID>" + Meas.ParentDatabase.ID + @"</DatabaseID>
        <CubeID>" + Meas.Parent.ID + @"</CubeID>
        <MeasureGroupID>" + Meas.ID + @"</MeasureGroupID>
      </Object>
      <Type>" + process + @"</Type>
    </Process>";
			if (Dim != null)
				return
					@"    <Process>
      <Object>
        <DatabaseID>" + Dim.Parent.ID + @"</DatabaseID>
        <DimensionID>" + Dim.ID + @"</DimensionID>
      </Object>
      <Type>" + process + @"</Type>
      <WriteBackTableCreation>UseExisting</WriteBackTableCreation>
     </Process>";
			return"";
		}
		
	}
	
	/// <summary>
	/// Основний клас для роботи з кубами
	/// </summary>
	static class XMLABuilder
	{
		static List<ProcessingOnLine> GlobalListOnLine = new List<ProcessingOnLine>();
		static List<ProcessingOnLine>  LocalListOnLine = new List<ProcessingOnLine>();
		static XmlaClient XMLACl;
		/// <summary>
		/// Пошук партиції за текучий період
		/// </summary>
		/// <param name="aMeasureGroup"></param>
		/// <param name="aThisYear"></param>
		/// <param name="aThisMonth"></param>
		/// <returns></returns>
		static private Partition CurrentPartitionFind(MeasureGroup aMeasureGroup, int aThisYear, int aThisMonth)
		{
			return PartitionFind(aMeasureGroup, aThisYear, aThisMonth);
		}
		
		/// <summary>
		///  Пошук партиції за попередній період
		/// </summary>
		/// <param name="aMeasureGroup"></param>
		/// <param name="aThisYear"></param>
		/// <param name="aThisMonth"></param>
		/// <returns></returns>
		static private Partition PreviousPartitionFind(MeasureGroup aMeasureGroup, int aThisYear, int aThisMonth)
		{
			int prevYear = aThisMonth == 1 ? aThisYear - 1 : aThisYear;
			int prevMonth = aThisMonth == 1 ? 12 : aThisMonth - 1;
			return PartitionFind(aMeasureGroup, prevYear, prevMonth);
		}

		/// <summary>
		/// Пошук партиції за конкретний рік і місяць
		/// </summary>
		/// <param name="aMeasureGroup"></param>
		/// <param name="aYear"></param>
		/// <param name="aMonth"></param>
		/// <returns></returns>
		static private Partition PartitionFind(MeasureGroup aMeasureGroup, int aYear, int aMonth)
		{
			string suffix = ToYYYYMM(aYear, aMonth);
			foreach (Partition p in aMeasureGroup.Partitions)
				if (p.Name.EndsWith(suffix))
					return p;
			return null;
		}

		/// <summary>
		/// Пошук партиції
		/// </summary>
		/// <param name="aMeasureGroup"></param>
		/// <param name="varDT"></param>
		/// <returns></returns>
		static private Partition PartitionFind(MeasureGroup aMeasureGroup, DateTime varDT)
		{
			string suffix = ToYYYYMM(varDT);
			foreach (Partition p in aMeasureGroup.Partitions)
				if (p.Name.EndsWith(suffix))
					return p;
			suffix = ToYYYYMMDD(varDT);
			foreach (Partition p in aMeasureGroup.Partitions)
				if (p.Name.EndsWith(suffix))
					return p;
			return null;
		}

		/// <summary>
		/// Створення помісячної партиції для шаблона template запит формату  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
		/// 20130601 - початковий період
		/// Доступо створення партіций як для Oracle так MSSQL
		/// </summary>
		/// <param name="aYear"></param>
		/// <param name="aMonth"></param>
		/// <param name="aForever"></param>
		/// <param name="aTableName"></param>
		/// <param name="aMeasugeGroup"></param>
		/// <param name="aTemplatePartition"></param>
		/// <param name="DateFieldName"></param>
		/// <param name="isOracle"></param>
		/// <returns>Партіцию</returns>
		static private Partition PartitionCreate(int aYear, int aMonth, bool aForever, string aTableName,
		                                         MeasureGroup aMeasugeGroup, Partition aTemplatePartition,string DateFieldName ,bool isOracle )
		{
			try
			{
				Partition p = aMeasugeGroup.Partitions.Add(aMeasugeGroup.ID + " " + ToYYYYMM(aYear, aMonth));
				p.AggregationDesignID = aTemplatePartition.AggregationDesignID;
				DateTime dStart = new DateTime(aYear, aMonth, 1);
				DateTime dEnd = new DateTime(aMonth == 12 ? aYear + 1 : aYear, aMonth == 12 ? 1 : aMonth + 1, 1);
				
				
				string sql = "select * from " + aTableName + " where " + DateFieldName + " >= " + (isOracle ? "to_date('" + ToYYYYMMDD(dStart) + "','YYYYMMDD')" : ToYYYYMMDD(dStart));
				if (!aForever)
					sql += " AND " + DateFieldName + " < " + (isOracle ? "to_date('" + ToYYYYMMDD(dEnd) + "','YYYYMMDD')" : ToYYYYMMDD(dEnd));
				p.Source = new QueryBinding(aTemplatePartition.DataSource.ID, sql);
				p.Slice="[Час].[Календар].[Місяць].&["+ToYYYYMM(aYear, aMonth)+"]";
				return p;
			}
			catch (Exception e)
			{
				Console.WriteLine("{0} Exception caught.", e);
			}
			return null;
		}
		

		
		/// <summary>
		/// Створення партицій для шаблона template_Month,template_Quarter,template_Year,template_4Week,template_Week
		/// Запит будь-якої складності де to_date('20130601','YYYYMMDD') -вказує на початок періоду і заміняється на реальний початок періоду а 
		/// to_date('00010101','YYYYMMDD') заміняється на кінець періоду
		/// Доступо створення партіций тільки для Oracle
		/// </summary>
		/// <param name="parMeasugeGroup"></param>
		/// <param name="parTemplatePartition"></param>
		/// <param name="parDStart"></param>
		/// <param name="parDEnd"></param>
		/// <param name="parType"></param>
		/// <returns>Партіцию</returns>
		static private Partition PartitionCreate( MeasureGroup parMeasugeGroup, Partition parTemplatePartition,DateTime parDStart, DateTime parDEnd, int parType)
		{
			try
			{
				string varSQL =( parTemplatePartition.Source as QueryBinding).QueryDefinition;
				string varStartDate= XMLABuilder.GetStartDate(varSQL);
				
				string varNewSQL =    varSQL.Replace(">= ",">=").Replace(">= ",">=").Replace(">= ",">=").Replace("< ","<").Replace("< ","<").Replace("< ","<").
					Replace(">=to_date('"+varStartDate ,">= to_date('"+ToYYYYMMDD(parDStart)).Replace("<to_date('00010101" ,"< to_date('"+ToYYYYMMDD(parDEnd));
				
				Partition p = parMeasugeGroup.Partitions.Add(parMeasugeGroup.ID + " " + ( parType>=10 ? ToYYYYMMDD(parDStart):ToYYYYMM(parDStart)));
				p.AggregationDesignID = parTemplatePartition.AggregationDesignID;
				p.Source = new QueryBinding(parTemplatePartition.DataSource.ID, varNewSQL);
				if(parType==1)
					p.Slice="[Час].[Календар].[Місяць].&["+ToYYYYMM(parDStart)+"]";
				return p;
			}
			catch (Exception e)
			{
				Console.WriteLine("{0} Exception caught.", e);
			}
			return null;
		}
		
		/// <summary>
		/// Пошук назви таблички для шаблона template запит формату  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
		/// </summary>
		/// <param name="aSql"></param>
		/// <returns></returns>
		private static string TableNameFromQueryGet(string aSql)
		{
			string sqlmod = aSql.Replace("\n", " ").Replace("\t", " ");
			string rest = sqlmod.Substring(sqlmod.ToLower().IndexOf("from") + 5).Trim();
			// Console.WriteLine("@" + rest.Substring(0, rest.IndexOf(" ")) + "@");
			return rest.Substring(0, rest.IndexOf(" ")).Trim();
		}

		/// <summary>
		/// Пошук  таблички для шаблона template запит формату  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
		/// </summary>
		/// <param name="aSql"></param>
		/// <returns></returns>
		private static string DateFieldNameFromQueryGet(string aSql)
		{
			string sqlmod = aSql.Replace("\n", " ").Replace("\t", " ");
			string rest = sqlmod.Substring(sqlmod.ToLower().IndexOf("and") + 4).Trim();
			// Console.WriteLine("@" + rest.Substring(0, rest.IndexOf(" ")) + "@");
			return rest.Substring(0, rest.IndexOf(">")).Trim();
		}
		
		/// <summary>
		/// Пошук  початкової дати для шаблона template запит формату  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
		/// </summary>
		/// <param name="aSql"></param>
		/// <returns></returns>
		public static string GetStartDate(string aSql)
		{
			string sqlmod = aSql.Replace("\n", " ").Replace("\t", " ").Replace(">= ",">=").Replace(">= ",">=").Replace(">= ",">=").Replace(">= ",">=");
			string rest = sqlmod.Substring(sqlmod.ToLower().IndexOf(">=to_date(") + 11).Trim();
			return rest.Substring(0, 8);
		}

		/// <summary>
		/// Провіряє чи запит створений для Oracle для шаблона template запит формату  SELECT * FROM "DW"."FACT_REST_DAY"   WHERE 1=0 and date_report>=to_date('20130601','YYYYMMDD')
		/// </summary>
		/// <param name="aSql"></param>
		/// <returns></returns>
		private static bool IsOracle(string aSql)
		{
			string sqlmod = aSql.Replace("\n", " ").Replace("\t", " ");
			return (sqlmod.ToLower().IndexOf("to_date(") >0);
		}


		/// <summary>
		/// Перевіряє чи в рядку pr=>31,1,7; крок в даному випадку 31 =  parStep
		/// </summary>
		/// <param name="varStr"></param>
		/// <param name="parStep"></param>
		/// <returns></returns>
		private static bool IsCurrentProcess(string varStr, int parStep )
		{
			if (parStep == 0 || parStep == 999) return true;
			if (varStr == null) { if (parStep == 11) return true; else return false; };
			if ((varStr.Trim().ToLower().Substring(0, 4) == "pr=>"))
			{
				if (Convert.ToInt16(strings.GetWordNum(strings.GetWordNum(varStr.Trim().Substring(4),1,";"),1,",")) == parStep)
					return true;
			}
			else
				if (parStep == 11) return true;
			return false;
		}

		/// <summary>
		/// Повертає скільки днів ще треба процесити партіцию за попередні періоди.(В рядку pr=>31,1,7; крок в даному випадку 1) 
		/// </summary>
		/// <param name="varStr"></param>
		/// <returns></returns>
		private static int DayProcess(string varStr)
		{   int varRez=0;
			if (varStr==null) return 20;
			if ((varStr.Trim().ToLower().Substring(0, 4) == "pr=>"))
			{
				try
				{
					varRez=Convert.ToInt16(strings.GetWordNum(strings.GetWordNum(varStr.Trim().Substring(4),1,";"),2,","));
				}
				catch
				{
					varRez=0;
				}
				return varRez;
				
			}
			return 0;
		}
		
		
		static private string ToYYYYMM(int aYear, int aMonth)
		{
			return aYear.ToString() + (aMonth < 10 ? "0" : "") + aMonth.ToString();
		}
		static private string ToYYYYMM(DateTime aDate)
		{
			int year = aDate.Year;
			int month = aDate.Month;
			
			return year.ToString() + (month < 10 ? "0" : string.Empty) + month.ToString() ;
			
		}

		static private string ToYYYYMMDD(DateTime aDate)
		{
			int year = aDate.Year;
			int month = aDate.Month;
			int day = aDate.Day;
			return year.ToString() + (month < 10 ? "0" : string.Empty) + month.ToString() +
				(day < 10 ? "0" : string.Empty) + day.ToString();
		}
		
		/// <summary>
		/// Визначає тип процесінгу. Якщо об'єкт (партіция, розмірність, міра) незапроцешина то повний процесінг, якщо ні - то дефаултний тип процесінгу.
		/// </summary>
		/// <param name="aObject"></param>
		/// <param name="aNeededProcessType"></param>
		/// <returns></returns>
		static private ProcessType SafeProcTypeGet(IProcessable aObject, ProcessType aNeededProcessType)
		{
			if (aObject.State != AnalysisState.Processed)
				return ProcessType.ProcessFull;
			return aNeededProcessType;
		}

		/// <summary>
		/// Очікуємо завершення розрахунку необхідних даних в БД 
		/// </summary>
		/// <param name="parStep"></param>
		static public void WaitOracle(int parStep)
		{
			if(GlobalVar.varWaitSQL!=null && GlobalVar.varConectSQL!=null)
				
			{
				DataSet dataSet = new DataSet();
				DataTable TTable = dataSet.Tables.Add("table");
				string varSqlConect=GlobalVar.varConectSQL;
				int state = 0;

				Log.log("Star WaitOracle");
				do
				{
					OleDbConnection myOleDbConnection = new OleDbConnection(GlobalVar.varConectSQL);
					OleDbDataAdapter adapterTable =
						new OleDbDataAdapter(GlobalVar.varWaitSQL, myOleDbConnection);
					adapterTable.Fill(TTable);
					foreach (DataRow row in TTable.Rows)
						state = Convert.ToInt16(row[0]);
					TTable.Clear();
					myOleDbConnection.Close();
					if (state == 0) Thread.Sleep(1000 * 5 * 60); //5 minets
				} while (state == 0);
				Log.log("End WaitOracle");
			}
		}
		


		/// <summary>
		/// Повертає тип партіциї
		/// </summary>
		/// <param name="g"></param>
		/// <returns></returns>
		static public int GetTypePartition(MeasureGroup g)
		{ Partition pTemplate;
			int varType=0;
			if((pTemplate = g.Partitions.FindByName("template")) != null)
				varType=1;
			else if((pTemplate = g.Partitions.FindByName("template_Month")) != null)
				varType=1;
			else if((pTemplate = g.Partitions.FindByName("template_Quarter")) != null)
				varType=2;
			else if((pTemplate = g.Partitions.FindByName("template_Year")) != null)
				varType=3;
			else if((pTemplate = g.Partitions.FindByName("template_4Week")) != null)
				varType=10;
			else if((pTemplate = g.Partitions.FindByName("template_Week")) != null)
				varType=11;
			return  varType;
		}
		
		/// <summary>
		/// Повертає Партіцию з шаблоном.
		/// </summary>
		/// <param name="g"></param>
		/// <returns></returns>
		static public Partition GetTemplatePartition(MeasureGroup g)
		{
			Partition pTemplate=null;
			if((pTemplate = g.Partitions.FindByName("template")) != null)
				return pTemplate;
			else if((pTemplate = g.Partitions.FindByName("template_Month")) != null)
				return pTemplate ;
			else if((pTemplate = g.Partitions.FindByName("template_Quarter")) != null)
				return pTemplate;
			else if((pTemplate = g.Partitions.FindByName("template_Year")) != null)
				return pTemplate ;
			else if((pTemplate = g.Partitions.FindByName("template_4Week")) != null)
				return pTemplate ;
			else if((pTemplate = g.Partitions.FindByName("template_Week")) != null)
				return pTemplate ;
			return  pTemplate;
		}
		
		/// <summary>
		/// Створює всі необхідні партіциї по всім групам мір в кубі 
		/// </summary>
		/// <param name="parCube"></param>
		static public void CreatePartition(Cube  parCube)
		{
			DateTime varNow = DateTime.Now ;
			DateTime varDateArxCube = new DateTime(1,1,1);
			CultureInfo provider = CultureInfo.InvariantCulture;
			if(parCube.Description!=null && strings.GetWordNum(parCube.Description,2,";").Length>5 &&  strings.GetWordNum(parCube.Description,2,";").Substring(0,5).ToUpper()=="ARX=>")
				varDateArxCube = DateTime.ParseExact(strings.GetWordNum(parCube.Description,2,";").Substring(5,10).ToUpper() ,"dd.MM.yyyy",provider);
			foreach (MeasureGroup g in parCube.MeasureGroups)
				try
			{
				
				Partition pTemplate=GetTemplatePartition(g) ;
				int varType=GetTypePartition(g);
				if (varType>0)
				{
					Partition currentPartition = null;

					if (pTemplate.Source is TableBinding)
						throw new ApplicationException(
							"template partition should have query binding \"select * from tablename where 1=0\"");
					//string table = TableNameFromQueryGet((pTemplate.Source as QueryBinding).QueryDefinition);
					string varStrStartDate = GetStartDate((pTemplate.Source as QueryBinding).QueryDefinition);
					DateTime varRealStartDate = new DateTime(Convert.ToInt32(varStrStartDate.Substring(0, 4)), Convert.ToInt32(varStrStartDate.Substring(4, 2)), Convert.ToInt32(varStrStartDate.Substring(6, 2)));
					DateTime varStartDate;
					if (GlobalVar.varIsArx)
					{
						if( GlobalVar.varArxDate == new DateTime(1,1,1) )
							varStartDate=varDateArxCube;
						else
							varStartDate=GlobalVar.varArxDate;
						/*if(varType==10) // 4week
                                	   	varStartDate= varRealStartDate-((varRealStartDate-varStartDate)/28+1)*28;
                                	else if(varType==11) //week
                                		varStartDate= varRealStartDate-((varRealStartDate-varStartDate)/7+1)*7;*/
					}
					else
						varStartDate = varRealStartDate;
					DateTime varEndDate = varStartDate;
					bool varIsOracle = IsOracle((pTemplate.Source as QueryBinding).QueryDefinition);
					string varDateField="",table="";
					if ( g.Partitions.FindByName("template")!=null) //TMP
					{
						varDateField = DateFieldNameFromQueryGet((pTemplate.Source as QueryBinding).QueryDefinition);//tmp
						table = TableNameFromQueryGet((pTemplate.Source as QueryBinding).QueryDefinition); //tmp
					}
					while (varStartDate <= varNow)
					{
						currentPartition = PartitionFind(g, varStartDate);
						varEndDate=CountNextPreviousDate(varStartDate,varType);
						if (currentPartition == null)
						{
							if ( g.Partitions.FindByName("template")==null)
								currentPartition = PartitionCreate(g, pTemplate, varStartDate, varEndDate ,varType );
							else
								currentPartition = PartitionCreate(varStartDate.Year, varStartDate.Month, false, table, g, pTemplate, varDateField, varIsOracle);
							
							currentPartition.Update();
							Console.WriteLine("Створено партіцию=>"+currentPartition.Name  ) ;
						}
						varStartDate = varEndDate;
					}

					
				}
			}
			catch (Exception e)
			{
				Log.log("Група мір=>" + g.Name +" Error =>"+ e.Message);
			}
			
		}
		
		/// <summary>
		/// Будує XMLA скріпт для процесінга.
		/// </summary>
		/// <param name="parList">Список об'єктів для процесінга</param>
		/// <returns></returns>
		static public string BildXMLA(List<ProcessingOnLine> parList)
		{
			StringBuilder script = new StringBuilder();
			script.AppendLine("<Batch xmlns=\"http://schemas.microsoft.com/analysisservices/2003/engine\">");
			script.AppendLine("<Parallel>");
			//"+(parProcessType == ProcessType.ProcessDefault?"": " maxParallel="" )+     "
			
			foreach (ProcessingOnLine task in parList)
				script.AppendLine(task.GetXMLA());
			
			script.AppendLine("</Parallel>");
			string varKeyErrorLogFile = ( GlobalVar.varKeyErrorLogFile==null? Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) :GlobalVar.varKeyErrorLogFile.Trim() )+
				"\\log\\Error_"+DateTime.Now.ToString("yyyyMMdd")+".log";
			
			script.AppendLine(@"
 <ErrorConfiguration>
      <KeyErrorLogFile>"+GlobalVar.varKeyErrorLogFile+@"</KeyErrorLogFile>
      <KeyErrorAction>ConvertToUnknown</KeyErrorAction>
      <KeyNotFound>IgnoreError</KeyNotFound>
      <NullKeyConvertedToUnknown>IgnoreError</NullKeyConvertedToUnknown>
</ErrorConfiguration>
");

			
			script.AppendLine("</Batch>");
			return script.ToString();
		}
		
		/// <summary>
		/// Процесить по списку
		/// </summary>
		/// <param name="parList">Список об'єктів для процесінга</param>
		static public void ProcessList(List<ProcessingOnLine> parList)
		{
			if(!RunXMLA(BildXMLA(parList)))
				foreach (ProcessingOnLine task in parList)
					task.Process();
		}
		
		/// <summary>
		/// Процесить все по списку розбиваючи їх згідно  parParallel
		/// </summary>
		/// <param name="parParallel">Скільки об'єктів процесити паралельно</param>
		/// <param name="parProcessType">Тип процесінгу</param>
		/// <param name="parInclude"></param>
		static public void ProcessPartXMLA(int parParallel=0, ProcessType parProcessType = ProcessType.ProcessDefault, bool parInclude = true)
		{
			if (parParallel==0)
				parParallel=GlobalVar.varMaxParallel;
			if (parParallel==0)
				return;
			
			int i=0;
			foreach (ProcessingOnLine task in GlobalListOnLine)
				if(( parProcessType == ProcessType.ProcessDefault) || (parInclude && parProcessType == task.ProcessingType) || (!parInclude && parProcessType != task.ProcessingType) )
			{
				if(i==0)
					LocalListOnLine.Clear();
				i++;
				LocalListOnLine.Add(task);
				if(i==parParallel)
				{
					ProcessList(LocalListOnLine);
					i=0;
					LocalListOnLine.Clear();
				}
			}
			if(i!=0)
				ProcessList(LocalListOnLine);
			
		}
		
		
		/// <summary>
		/// Виконує XMLA
		/// </summary>
		/// <param name="parXMLA">XMLA Код</param>
		/// <param name="parStep">крок процесінгу</param>
		/// <returns></returns>
		static public bool RunXMLA(string parXMLA, int parStep = 0)
		{
			
			
			if( !(DateTime.Now.Hour>=GlobalVar.varTimeStart) && (DateTime.Now.Hour <= GlobalVar.varTimeEnd))
			{
				Log.log("Час за межами діапазону("+GlobalVar.varTimeStart.ToString().Trim() +"-"+GlobalVar.varTimeEnd.ToString().Trim()+") Зараз=>" + DateTime.Now.ToString() );
				return true;
			}
			try
			{
				string varRez;
				//string varFile = GlobalVar.varFileLog;
				
				Log.log(parXMLA);
				XMLACl.Execute(parXMLA,"",out varRez,false,false );
				Log.log("Rez XMLA=>"+varRez);

				if (varRez.IndexOf("<Error")==-1)
					return true;
				else
					return false;
			}
			catch
			{
				return false;
			}
			
		}
		
		/// <summary>
		/// 
		/// </summary>
		/// <param name="parMetod"></param>
		/// <param name="parStep"></param>
		/// <param name="parXMLA"></param>
		static public void ProcessList(int parMetod=0, int parStep = 0, int parXMLA=1)
		{
			/* Невдалий метод розпаралелення С#
   	ParallelOptions options = new ParallelOptions {  MaxDegreeOfParallelism = 4 , };
		Parallel.For(0, GlobalListOnLine.Count, options, i=>
  		            {
  		             	if(GlobalListOnLine[i].ProcessingType==ProcessType.ProcessUpdate)
  		             		GlobalListOnLine[i].Process(parMetod,parStep);

  		             });*/

			// Процесимо діменшини
			/*  	if(!RunXMLA(BildXMLA(ProcessType.ProcessUpdate),parStep))
  		foreach (ProcessingOnLine task in GlobalListOnLine)
  			if(task.ProcessingType==ProcessType.ProcessUpdate)
  				task.Process(parMetod,parStep);*/
			
			WaitOracle(parStep);

			if(parXMLA==0)
			{
				foreach (ProcessingOnLine task in GlobalListOnLine)
					if(task.ProcessingType!=ProcessType.ProcessUpdate)
						task.Process(parMetod);
			}
			else
			{
				/*       if(!RunXMLA(BildXMLA(ProcessType.ProcessUpdate,false),parStep))
 		  foreach (ProcessingOnLine task in GlobalListOnLine)
  			if(task.ProcessingType!=ProcessType.ProcessUpdate)
  				task.Process(parMetod,parStep);*/
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parPartition"></param>
		/// <returns></returns>
		static public DateTime GetDateStartPartition(Partition parPartition)
		{
			int varType=GetTypePartition(parPartition.Parent);
			string varSTR = parPartition.Name.Substring(parPartition.Name.Length-(varType<10?  6 :8)) + (varType<10? "01":"") ;
			try
			{
				return  new DateTime( Convert.ToInt32(varSTR.Substring(0,4) ),Convert.ToInt32(varSTR.Substring(4,2)), Convert.ToInt32(varSTR.Substring(6,2)));
			}
			catch
			{
				return new DateTime(1,1,1);
			}
		}
		
		static public DateTime CountNextPreviousDate( DateTime parStartDate, int parType,int parCoef =1)
		{
			switch (parType)
			{
				case 1 :
					return parStartDate.AddMonths(parCoef*1);
				case 2 :
					return parStartDate.AddMonths(parCoef*3);
				case 3 :
					return parStartDate.AddYears(parCoef*1);
				case 10 :
					return parStartDate.AddDays(parCoef*28);
				case 11 :
					return parStartDate.AddDays(parCoef*7);
			}
			return new DateTime(9999,12,31);
		}
		
		
		static public Partition PreviousLastPartition( MeasureGroup parMG)
		{
			int varType=GetTypePartition(parMG);
			Partition varCurPar = FindLastPartition(parMG) ;
			DateTime varDate = CountNextPreviousDate(GetDateStartPartition(varCurPar),varType,-1);
			return PartitionFind(parMG,varDate);
		}
		static public Partition FindLastPartition( MeasureGroup parMG)
		{
			int varType=GetTypePartition(parMG);
			DateTime varMax = new DateTime(1,1,1),varCur ;
			Partition rezPar = null;
			foreach ( Partition p in parMG.Partitions)
			{
				varCur=GetDateStartPartition(p);
				if(varCur>varMax)
				{
					rezPar=p;
					varMax=varCur;
				}
			}
			return rezPar;
		}
		
		static public int CountPartition(MeasureGroup parG,bool parProcess = false)
		{
			int i=0;
			foreach (Partition p in parG.Partitions)
				if( p.State == AnalysisState.Unprocessed ||  !parProcess )
					i++;
			return i;
		}

		static public double CountMeasureSize(MeasureGroup parG)
		{
			double s=0;
			foreach (Partition p in parG.Partitions)
				s+=(p.EstimatedSize/(1024*1024));
			return s;
		}
		
		static public double CountCubeSize(Cube parCube)
		{
			double s=0;
			foreach (MeasureGroup varMG in parCube.MeasureGroups)
				s+= CountMeasureSize(varMG);
			return s;
		}
		
		
		static public void ProcessPartition(Cube  parCube,int parStep)
		{
			if( GlobalVar.varIsArx && strings.GetWordNum(parCube.Description,2,";").Substring(0,5).ToUpper()!="ARX=>" )
				return;

			foreach (MeasureGroup g in parCube.MeasureGroups)
			{
				if ( IsCurrentProcess(g.Description,parStep))
				{
					if (GetTypePartition(g)==0)
					{
						if( g.State==AnalysisState.Unprocessed ||  g.State==AnalysisState.PartiallyProcessed ||  (parStep!=0 && parStep!=999))
							//                 GlobalList.Add(new ProcessingTask(ProcessType.ProcessFull, parCube.Parent.ID, parCube.ID, g.ID));
							if (g.State==AnalysisState.Unprocessed || CountPartition(g)<=2)
								GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube, g));
							else
								foreach (Partition p in g.Partitions)
									if( p.State == AnalysisState.Unprocessed || ( p.Description==null?null: p.Description.Substring(0,8) ) == "current;"  )
										GlobalListOnLine.Add(new ProcessingOnLine(  GlobalVar.varProcessCube, p));
						
						
					}
					else
					{
						Partition varCurrentPartition=null,varPreviousPartition=null;
						if ( parStep!=0 && parStep!=999)
						{
							varCurrentPartition = FindLastPartition(g);
							//Якщо дані в партіциї
							if(GetDateStartPartition(varCurrentPartition)> DateTime.Now.AddDays(- (DayProcess(g.Description)==0? GlobalVar.varDayProcess: DayProcess(g.Description))) )
								varPreviousPartition =  PreviousLastPartition(g);
						}
						foreach (Partition p in g.Partitions)
						{
							if( (p == varCurrentPartition) || (p == varPreviousPartition ) || (p.State == AnalysisState.Unprocessed) || GetDateStartPartition(p)>=GlobalVar.varDateStartProcess )
								GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube, p));
							else if (parStep!=0) //(p.State == AnalysisState. )
							{
								//                                        GlobalListOnLine.Add(new ProcessingOnLine(ProcessType.ProcessIndexes, p));
							}
						}
					}
				}
			}
		}
		
		/// <summary>
		/// Швидке "підняття" куба. Все необхідне для того щоб куб був в ON-Line
		/// </summary>
		/// <param name="parCube"></param>
		static public void QuickUp1(Cube  parCube)
		{
			Partition varTemplatePartition;
			foreach (MeasureGroup g in parCube.MeasureGroups)
			{
				if(!g.IsLinked || (g.IsLinked  && (StateMeasure(parCube.Parent,g.Source.CubeID ,g.Source.MeasureGroupID) == AnalysisState.Processed) ))
					if (GetTypePartition(g)==0)
				{
					if(g.State == AnalysisState.Unprocessed )
						if( (varTemplatePartition=g.Partitions.FindByName("template_QuickUp") ) != null)
							
							GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube, varTemplatePartition ));
						else
							GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube, g));
				}
				else
				{
					varTemplatePartition=GetTemplatePartition(g);
					if (varTemplatePartition!=null && varTemplatePartition.State == AnalysisState.Unprocessed )
						GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube,varTemplatePartition));
				}
			}
		}

		/// <summary>
		/// Швидке "підняття" куба. Все необхідне для того щоб були дані в останній і попередній партіції.
		/// </summary>
		/// <param name="parCube"></param>
		static public void QuickUp2(Cube  parCube)
		{
			Partition varLastPartition;
			foreach (MeasureGroup g in parCube.MeasureGroups)
			{
				if(!g.IsLinked || (g.IsLinked  && (StateMeasure(parCube.Parent,g.Source.CubeID ,g.Source.MeasureGroupID) == AnalysisState.Processed) ))
					if (GetTypePartition(g)==0)
						foreach (Partition p in g.Partitions)
							if( p.State == AnalysisState.Unprocessed )
								GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube, p));
							else
				{
					varLastPartition=FindLastPartition(g);
					if (varLastPartition!=null && varLastPartition.State == AnalysisState.Unprocessed )
						GlobalListOnLine.Add(new ProcessingOnLine(GlobalVar.varProcessCube,varLastPartition));
				}
			}
		}
		
		
		/// <summary>
		/// Швидке "підняття" куба.
		/// </summary>
		/// <param name="parCube"></param>
		static public void QuickUp(Cube  parCube)
		{
			GlobalListOnLine.Clear();
			QuickUp1(parCube);
			ProcessPartXMLA();
			GlobalListOnLine.Clear();

			QuickUp2(parCube);
			
			ProcessPartXMLA();
			GlobalListOnLine.Clear();
			
		}
		
		/// <summary>
		/// Добавляємо в список процешення в режимі Index всі  Групи мір необхідне після процешення розмірностей.
		/// </summary>
		/// <param name="parCube">Куб</param>
		static public void ProcessIndex(Cube  parCube)
		{
			foreach (MeasureGroup g in parCube.MeasureGroups)
				GlobalListOnLine.Add(new ProcessingOnLine(ProcessType.ProcessIndexes, g));
		}
		
		/// <summary>
		/// Стан групи мір.
		/// </summary>
		/// <param name="varDB"></param>
		/// <param name="parCube"></param>
		/// <param name="parMeasure"></param>
		/// <returns></returns>
		static public AnalysisState StateMeasure(Database varDB, string parCube, string parMeasure )
		{
			return varDB.Cubes[parCube].MeasureGroups[parMeasure].State;
			
		}
		
		/// <summary>
		/// Основна процедура процесінга всіх кубів.
		/// </summary>
		/// <param name="parConnectionString"></param>
		/// <param name="parDB"></param>
		/// <param name="parCube"></param>
		/// <param name="parStep"></param>
		/// <param name="parMetod"></param>
		static public void Process(string parConnectionString, string parDB, string parCube,int parStep = 0,int parMetod=0)
		{
			
			Server s = new Server();
			try
			{
				s.Connect(parConnectionString);
				Database varDB = s.Databases.FindByName(parDB);

				XMLACl = new XmlaClient();
				XMLACl.Connect(parConnectionString);

				//                AddSlicePartition(varDB); //TMP
				
				
				if(parStep<-9990)
				{
					foreach (Cube varCube in varDB.Cubes)
					{
						Log.log("Куб=>"+ varCube.Name  + "\t Size=>"+ CountCubeSize(varCube).ToString() + " :"+varCube.State.ToString()+" :"+varCube.Description );
						if(varCube.State != AnalysisState.Processed || parStep==-9998)
						{
							foreach (MeasureGroup varMG in varCube.MeasureGroups)
							{
								if( varMG.State!=AnalysisState.Processed || parStep==-9998 )
									Log.log(" Група мір =>"+ varMG.Name + "\t Size=>"+ CountMeasureSize(varMG).ToString() +  " :"+ varMG.State.ToString()+" :"+ (GetTemplatePartition(varMG)== null ? "" : GetTemplatePartition(varMG).Name)+" :" +varMG.Description );
							}
						};
					}
					return;
				}
				// dimensions processing   || Процесимо діменшини.
				if ( parCube== null && GlobalVar.varIsProcessDimension)
				{
					foreach (Dimension dim in varDB.Dimensions)
						GlobalListOnLine.Add(new ProcessingOnLine(SafeProcTypeGet(dim, GlobalVar.varProcessDimension),dim));
					ProcessPartXMLA();
					GlobalListOnLine.Clear();
				}
				
				//Створення нових партіций.
				if(parCube==null)
					foreach (Cube varC in varDB.Cubes)
						CreatePartition(varC);
					else
						CreatePartition(varDB.Cubes.FindByName(parCube));

				// Швидке підняття куба
				if(parCube==null)
					foreach (Cube varC in varDB.Cubes)
						QuickUp(varC);
					else
						QuickUp(varDB.Cubes.FindByName(parCube));
				

				//Процес куба.
				if(parCube==null)
					foreach (Cube varCube in varDB.Cubes)
						ProcessPartition(varCube, parStep) ;
					else
				{
					Cube varCube =varDB.Cubes.FindByName(parCube);
					ProcessPartition(varCube, parStep) ;
				}
				WaitOracle(parStep);
				ProcessPartXMLA();

				// Всі куби процесимо INDEX (Якщо процесили діменшини)
				//ProcessList(parMetod,parStep,parXMLA);
				if ( parCube== null &&  GlobalVar.varIsProcessDimension)
				{
					foreach (Cube varCube in varDB.Cubes)
						ProcessIndex(varCube);
					ProcessPartXMLA();
					//                 	ProcessList(parMetod,parStep,1);
					GlobalListOnLine.Clear();
				}
				
			}
			finally
			{
				/*                s.Disconnect();
                clnt.Disconnect(); */
				
			}
			;
		}
		
/*
		static public void AddSlicePartition(Database parDB)
		{
			foreach (Cube varC in parDB.Cubes)
				foreach (MeasureGroup g in varC.MeasureGroups)
			{
				int varType=GetTypePartition(g);
				if (varType>1)
					foreach (Partition p in g.Partitions)
						if( p.Slice != null && p.Slice.Trim().Length>0 )
				{
					p.Slice=  "";
					p.Update();
				}
				
				
          		if((p.Slice == null || p.Slice.Trim().Length==0 ) && p.Name.ToLower().Substring(0,8 )!="template")
        {
        	string varS=p.Name.Substring(p.Name.Length-6,6);
        	p.Slice=  "[Час].[Календар].[Місяць].&["+varS+"]";
        	p.Update();
        }
				 
			}
			
		}*/
		
	}
	
	/// <summary>
	/// Примітивний клас для роботи с XML
	/// </summary>
	class MyXML
	{
		XmlDocument doc = new XmlDocument();
		public MyXML(string varFileName="")
		{
			if(varFileName.Trim().Length>0)
				doc.Load(varFileName);
		}
		public string GetVar(string parKey1,string parKey2="")
		{
			try
			{
				if(parKey2.Length==0)
					return doc.DocumentElement.SelectSingleNode(parKey1).InnerText.Trim() ;
				else
					return doc.DocumentElement.SelectSingleNode(parKey1).SelectSingleNode(parKey2).InnerText.Trim() ;
			}
			catch ( Exception ex)
			{
				return null;
			}
			
		}
		public string GetAttribute(string parAttribute, string parKey1, string parKey2="" )
		{
			try
			{
				if(parKey2.Length==0)
					return doc.DocumentElement.SelectSingleNode(parKey1).Attributes[parAttribute].Value.Trim() ;
				else
					return doc.DocumentElement.SelectSingleNode(parKey1).SelectSingleNode(parKey2).Attributes[parAttribute].Value.Trim() ;
			}
			catch ( Exception ex)
			{
				return null;
			}
			
			
		}
	}
	
	class MyXMLA
	{
		public static void SetProcessTypeDimension(string parStr)
		{
			string varStr = parStr.Trim().ToUpper();
			GlobalVar.varIsProcessDimension = true;
			if(varStr=="NONE")
				GlobalVar.varIsProcessDimension = false;
			else if(varStr=="UPDATE")
				GlobalVar.varProcessDimension = ProcessType.ProcessUpdate;
			else if(varStr=="FULL")
				GlobalVar.varProcessDimension = ProcessType.ProcessFull;
		}
		public static void SetProcessTypeCube(string parStr)
		{
			string varStr = parStr.Trim().ToUpper();
			if(varStr=="NONE")
				GlobalVar.varIsProcessCube = false;
			else if(varStr=="DATA")
				GlobalVar.varProcessCube = ProcessType.ProcessData;
			else if(varStr=="FULL")
				GlobalVar.varProcessCube = ProcessType.ProcessFull;
		}
		
		
		
	}
	
	/// <summary>
	/// Клас для управління Windows службою
	/// </summary>
	class Services
	{
		private ServiceController sc;
		public string log;
		
		public void Log(string parLog ="")
		{
			DateTime varDT =DateTime.Now;
			log= varDT.ToString() + "=>" + (parLog.Length>0?parLog +"=>":"") + sc.Status.ToString() + "\n";
		}
		
		public Services(string varServiceName="MSSQLServerOLAPService",string varServer="localhost")
		{
			sc = new ServiceController(varServiceName,varServer);
		}
		
		public bool WaitServiceInProces(int parWaitSec=200)
		{
			Log("WaitServiceInProces");
				do
				{
					Thread.Sleep(1000);
					sc.Refresh();
					Log();
				}
				while ((sc.Status == ServiceControllerStatus.StopPending || sc.Status == ServiceControllerStatus.StartPending ) && parWaitSec-- > 0);
				return (parWaitSec>0);
		}
		
		public bool Start(int parWaitSec=200)
		{
			Log("Start");
			try
			{
				if (!WaitServiceInProces(parWaitSec)) return false;

				if (sc.Status == ServiceControllerStatus.Stopped)
				{
					sc.Start();
					do
					{
						Thread.Sleep(1000);
						sc.Refresh();
						Log();
					}
					while (sc.Status != ServiceControllerStatus.Running && parWaitSec-- > 0);
				}
				return  (parWaitSec>0);
			}
			catch(Exception ex)
			{
				return false;
			}
		}
		
		public bool Stop(int parWaitSec=200)
		{
			Log("Stop");
			try
			{
				if (!WaitServiceInProces(parWaitSec)) return false;
				
				if (sc.Status == ServiceControllerStatus.Running)
				{
					sc.Stop();
					do
					{
						Thread.Sleep(1000);
						sc.Refresh();
						Log();
					}
					while (sc.Status != ServiceControllerStatus.Stopped && parWaitSec-- > 0);
				}
				return  (parWaitSec>0);
			}
			catch(Exception ex)
			{
				return false;
			}
		}
		public bool ReStart(int parWaitSec=200)
		{
			log="";
			return Stop(parWaitSec)&&Start(parWaitSec);
		}
		public bool IsStart()
		{
			sc.Refresh();
			return (sc.Status == ServiceControllerStatus.Running);
		}
		
	}
	
}