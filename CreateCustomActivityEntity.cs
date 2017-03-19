
using System;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Messages;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Xrm.Tooling.Connector;
using Microsoft.Crm.Sdk.Samples;

namespace WebAPI
{

    public class CreateCustomActivityEntity
    {
        #region Class Level Members
        /// <summary>
        /// Stores the organization service proxy.
        /// Store the Custom Entity Name.
        /// Store the global variable.
        /// Store the Output of function in List l.
        /// </summary>
        public static IOrganizationService _service = null;
        private string customEntityName;
        public static string SQL_IP;
        public static string SQL_Port;
        public static string SQL_UN;
        public static string SQL_Pass;
        public static string SQL_DB;
        public static string SQL_TableName;
        public static string CRM_Server;
        public static string CRM_Office;
        public static string CRM_UN;
        public static string CRM_Pass;
        public static string CRM_TableName;
        public static string CRM_Suffix;
        public static OrganizationServiceProxy _serviceProxy = null;
        public List<string> l = new List<string>();


        #endregion Class Level Members

        #region Database Schema Retrive
        ///<summary>
        ///Create Custom Entity.
        ///</summary>
        public List<string> createCustomEntity(DataSet ds, string CRM_TableName, string CRM_Suffix)
        {
            List<string> lst = new List<string>();
            String prefix = "new_";
            ExecuteMultipleRequest SchemaCreate = null;
            customEntityName = prefix + CRM_TableName.ToLower() + CRM_Suffix;



            SchemaCreate = new ExecuteMultipleRequest()
            {
                // Assign settings that define execution behavior: continue on error, return responses. 
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = false,
                    ReturnResponses = true

                },
                // Create an empty organization request collection.
                Requests = new OrganizationRequestCollection()

            };
            int f = 1;
            foreach (DataRow row in ds.Tables[SQL_TableName].Rows)
            {
                string t = prefix + row[0].ToString() + CRM_Suffix; //add Prifix & Suffix for creating Entity and it's attributes.
                lst.Add(t); //Add Attributes name in List for use it further in insertion of data into these entities.
                if (f > 0)  //Set flag for first create Custom Entity than create attribute of it.
                {
                    CreateEntityRequest request = new CreateEntityRequest
                    {
                        HasNotes = true,
                        HasActivities = false,

                        Entity = new EntityMetadata
                        {
                            IsActivity = false,
                            SchemaName = customEntityName,
                            DisplayName = new Label(CRM_TableName, 1033),
                            DisplayCollectionName = new Label(CRM_TableName, 1033),
                            OwnershipType = OwnershipTypes.UserOwned,
                            IsAvailableOffline = true,

                        },
                        PrimaryAttribute = new StringAttributeMetadata  //create primary attribute, string type.
                        {
                            SchemaName = t,
                            RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                            MaxLength = 200,
                            DisplayName = new Label(row[0].ToString(), 1033),
                            Description = new Label(row[0].ToString(), 1033)
                        }
                    };

                    _service.Execute(request);//Execute Request for create Entity. 
                    f--;
                    continue;
                }

                switch (row[1].ToString())  //Use Switch case for Dynamically Create Attributes accourding to there data types in SQL schema fields.
                {
                    case "nvarchar":
                    case "text":

                        CreateAttributeRequest M_ID = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            Attribute = new StringAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                                MaxLength = 2000,
                                Format = StringFormat.Text,
                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033)
                            }
                        };
                        SchemaCreate.Requests.Add(M_ID);  //Add Request to MultipleRequest.
                        break;
                    case "datetime":
                        CreateAttributeRequest M_CreatedDate = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            //DateTimeAttributeMetadata
                            Attribute = new StringAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                                //Format = DateTimeFormat.DateAndTime,
                                //DateTimeBehavior =DateTimeBehavior.UserLocal,
                                MaxLength = 50,
                                Format = StringFormat.Text,
                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033)

                            }
                        };
                        SchemaCreate.Requests.Add(M_CreatedDate);
                        // _service.Execute(M_CreatedDate);
                        break;
                    case "smalldatetime":
                        CreateAttributeRequest M_CreatedSmallDate = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            //DateTimeAttributeMetadata
                            Attribute = new StringAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                                //Format = DateTimeFormat.DateOnly,
                                //DateTimeBehavior = DateTimeBehavior.DateOnly,
                                MaxLength = 2000,
                                Format = StringFormat.Text,
                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033)

                            }
                        };
                        SchemaCreate.Requests.Add(M_CreatedSmallDate);
                        //_service.Execute(M_CreatedSmallDate);
                        break;
                    case "int":
                        CreateAttributeRequest M_Integer = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            Attribute = new IntegerAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033)

                            }
                        };
                        SchemaCreate.Requests.Add(M_Integer);
                        //_service.Execute(M_Integer);
                        break;
                    case "money":
                        CreateAttributeRequest createBalanceAttributeRequest = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            Attribute = new MoneyAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),
                                Precision = 1,
                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033),

                            }
                        };
                        SchemaCreate.Requests.Add(createBalanceAttributeRequest);
                        // _service.Execute(createBalanceAttributeRequest);
                        break;
                    case "decimal":
                        CreateAttributeRequest M_Decimal = new CreateAttributeRequest
                        {
                            EntityName = customEntityName,
                            Attribute = new DecimalAttributeMetadata
                            {
                                SchemaName = t,
                                RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.None),

                                DisplayName = new Label(row[0].ToString(), 1033),
                                Description = new Label(row[0].ToString(), 1033),

                            }
                        };
                        SchemaCreate.Requests.Add(M_Decimal);
                        // _service.Execute(M_Decimal);
                        break;
                }
            }
            _serviceProxy.Timeout = new TimeSpan(0, 10, 0); //Set 10min Timeout for response MultipleRequest. 

            ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)_service.Execute(SchemaCreate); //Execute Multiple Request to create all attributes
            return lst;
        }
        #endregion

        #region Upload sql Data on Custom Entity
        /// <summary>
        /// Retrive data from sql into dataset and upload on created custom entity. 
        /// </summary>
        /// <param name="serverConfig"></param>
        /// <param name="schemas"></param>
        public string uploadData(DataSet dss, string CRM_Table, string Suffix)
        {
            string a = null;
            int x = 0;
            string prefix = "new_";
            customEntityName = prefix + CRM_Table + Suffix;
            ExecuteMultipleRequest UploadMulti = null; //Generate Multiple Request.

            //Set setting for multiple Request.Continue on error & Return Responses
            UploadMulti = new ExecuteMultipleRequest()
            {
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = false,
                    ReturnResponses = true
                },
                //create request collection. 
                Requests = new OrganizationRequestCollection()

            };
            ExecuteMultipleResponse responseWithResults;
            // This statement is required to enable early-bound type support.
            using (SqlConnection cnn = new SqlConnection("Data Source=" + SQL_IP + "," + SQL_Port + ";user=" + SQL_UN + ";password=" + SQL_Pass + ";database=" + SQL_DB + "; "))
            {
                SqlDataAdapter da = new SqlDataAdapter("SELECT * FROM [" + SQL_TableName + "]", cnn);//Fetch data from table SQL_TableName.
                DataSet ds = new DataSet();
                da.Fill(ds, SQL_TableName); //Fill Data into DataSet ds.
                foreach (DataRow row in ds.Tables[SQL_TableName].Rows)
                {
                    Entity Merc = new Entity(customEntityName); //Create Entity which is type of customEntity.
                    int i = 0, f = 0;
                    foreach (DataRow roww in dss.Tables[SQL_TableName].Rows) //Dynamically Mapping data to there relative Fields
                    {
                        string t = prefix + roww[0].ToString() + Suffix;
                        switch (roww[1].ToString())
                        {

                            case "nvarchar":
                            case "text":
                                Merc[t.ToLower()] = row[i].ToString();
                                i++;
                                break;
                            case "datetime":
                                // a= DateTime.Parse(row[i].ToString()).ToUniversalTime().ToString();
                                //Merc[schemas[i].ToLower()] = DateTime.Parse(row[i].ToString());
                                Merc[t.ToLower()] = row[i].ToString();
                                i++;
                                break;
                            case "smalldatetime":
                                // Merc[schemas[i].ToLower()] = DateTime.Parse(row[i].ToString());
                                Merc[t.ToLower()] = row[i].ToString();
                                i++;
                                break;
                            case "int":
                                Merc[t.ToLower()] = Convert.ToInt32(row[i]);
                                i++;
                                break;
                            case "money":
                                Merc[t.ToLower()] = new Money(Convert.ToDecimal(row[i].ToString()));
                                i++;
                                break;
                            case "decimal":
                                Merc[t.ToLower()] = row[i];
                                i++;
                                break;
                        }
                    }
                    // Add a CreateRequest for each entity to the request collection.
                    CreateRequest createRequest = new CreateRequest { Target = Merc };
                    UploadMulti.Requests.Add(createRequest);//Add Request to varibale UploadMulti
                    f++;

                    if (UploadMulti.Requests.Count == 1000)//if 1000 records are added than execute request.
                    {
                        _serviceProxy.Timeout = new TimeSpan(0, 10, 0);//set 10 min timout for executing multiple request

                        responseWithResults = (ExecuteMultipleResponse)_service.Execute(UploadMulti);
                        x = x + responseWithResults.Responses.Count;
                        UploadMulti.Requests.Clear();//Clear all request from UploadMulti.
                        f = 0;
                    }

                }
                if (UploadMulti.Requests.Count > 0)
                {

                    responseWithResults = (ExecuteMultipleResponse)_service.Execute(UploadMulti);
                    x = x + responseWithResults.Responses.Count;
                    UploadMulti.Requests.Clear();

                }
                a = "DataUpload" + x;
            }

            return (a);
        }
        #endregion


        #region CRM Connection
        ///<summary>
        ///Create Connection to CRM and Get service Config
        ///</summary>
        public IOrganizationService CRMConnect(string R_CRM_SERVER, string R_CRM_OFFICE, string R_CRM_UN, string R_CRM_Pass)
        {
            CRM_Server = R_CRM_SERVER.Trim();
            CRM_Office = R_CRM_OFFICE.Trim();
            CRM_UN = R_CRM_UN.Trim();
            CRM_Pass = R_CRM_Pass.Trim();
            ServerConnection serverConnect = new ServerConnection();
            ServerConnection.Configuration serverConfig = serverConnect.GetServerConfiguration(CRM_Server, CRM_Office, CRM_UN, CRM_Pass);

            try
            {
                using (_serviceProxy = new OrganizationServiceProxy(serverConfig.OrganizationUri, serverConfig.HomeRealmUri, serverConfig.Credentials, serverConfig.DeviceCredentials))
                {
                    // This statement is required to enable early-bound type support.
                    _service = (IOrganizationService)_serviceProxy;

                }
            }
            catch (Exception e)
            {
                string x = "Attribute Problem";
                l.Add(x);
                throw (e);
            }
            return _service;
        }
        #endregion
        #region MainM
        public void MainM(string R_SQL_IP, string R_SQL_Port, string R_SQL_UN, string R_SQL_Pass, string R_SQL_DB, string R_SQL_TableName, string R_CRM_TableName, string R_CRM_Suffix, IOrganizationService ServerService)
        {

            SQL_IP = R_SQL_IP.Trim();
            SQL_Port = R_SQL_Port.Trim();
            SQL_UN = R_SQL_UN.Trim();
            SQL_Pass = R_SQL_Pass.Trim();
            SQL_DB = R_SQL_DB.Trim();
            SQL_TableName = R_SQL_TableName.Trim();
            CRM_TableName = R_CRM_TableName.Trim();
            CRM_Suffix = R_CRM_Suffix.Trim();
            _service = ServerService;
            DataSet DS = new DataSet();


            try
            {
                using (SqlConnection cnn = new SqlConnection("Data Source=" + SQL_IP + "," + SQL_Port + ";user=" + SQL_UN + ";password=" + SQL_Pass + ";database=" + SQL_DB + "; "))
                {
                    //Retrive fields ,datatype of fields and maximum length of data.
                    SqlDataAdapter da = new SqlDataAdapter("SELECT column_name as 'Column Name', data_type as 'Data Type',character_maximum_length as 'Max Length' FROM information_schema.columns WHERE table_name = '" + SQL_TableName + "' ; ", cnn);
                    da.Fill(DS, SQL_TableName);

                }
            }
            catch (Exception ex)
            {
                l.Add("SQL_Schema" + ":::" + ex.Message);
            }
            try
            {

                Stopwatch s = new Stopwatch();
                s.Start();

                CreateCustomActivityEntity app = new CreateCustomActivityEntity();
                s.Stop();
                List<string> schema = new List<string>();
                l.Add("Configuration time::" + s.Elapsed);
                s.Reset();
                s.Start();
                schema = app.createCustomEntity(DS, CRM_TableName, CRM_Suffix);
                s.Stop();
                l.Add("Custom Entity Creation Time::" + s.Elapsed);
                s.Reset();
                s.Start();
                l.Add(app.uploadData(DS, CRM_TableName, CRM_Suffix));
                s.Stop();
                l.Add("Data Insertion Time:" + s.Elapsed);

            }
            catch (FaultException<Microsoft.Xrm.Sdk.OrganizationServiceFault> ex)
            {
                l.Add("ORG_serviceProxysFault:" + ex.Message + ":::" + ex.StackTrace);
            }
            catch (System.TimeoutException ex)
            {
                l.Add("TimeOut:" + ex.Message + "::::" + ex.StackTrace);
            }

            catch (System.Exception ex)
            {
                l.Add("Other:" + ex.Message + "::" + ex.HelpLink);
            }
            // return(l);
        }

        public static void Main(string[] args) {

            CreateCustomActivityEntity y = new CreateCustomActivityEntity();
      

        _service = y.CRMConnect("crm8.dynamics.com", "y", "karan@celebalms.onmicrosoft.com", "Qwertyuiop12345");
         y.MainM("104.197.61.242","14344","sa","mssql@123","salesforcedbdump","Merchant__c_sfdc","zoom","_sfdc",_service);
        
        }
        #endregion

    }

}