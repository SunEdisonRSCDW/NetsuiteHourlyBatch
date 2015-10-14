
/****

********SunEdison*********

 : Anurag Bhardwaj

Version                     Author                      Change
1.0                     Anurag Bhardwaj             Initial Version
1.1                     Anurag Bhardwaj             Array Info Object Added
1.2                     Anurag Bhardwaj             Promise Data Object Added
1.3                     Anurag Bhardwaj             Case Data Info Object Added
1.4                     Anurag Bhardwaj             Sales Order Object Added
1.5                     Anurag Bhardwaj             Total Contract Price field added into Site Object.
1.6                     Anurag Bhardwaj             Added another criteria called recordId to fetch records from netsuite.

****/

var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
    region = "us-east-1";
    console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

//Requiring aws-sdk. 
var aws = require('aws-sdk');
aws.config.update({
    region : region
});

//Requiring S3 module. 
var s3 = new aws.S3({
    apiVersion : '2006-03-01',
    region : region
});
//Requiring dynamoDB module. 
var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : region
});

//Requiring SNS module. 
var sns = new aws.SNS({
    apiVersion : '2010-03-31',
    region : region
});

//Importing exteral file constants. 
require('./constants');

//Importing kmsCrypto. 
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);

var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');

//Importing postgre. 
var pg = require('pg');

//Importing https. 
var http = require('https');

var upgrade = require('./upgrades');
var zlib = require('zlib');

//Importing querystring to get more DB calls into the script.
var querystring = require('querystring');

var responseString = '';
var responseStringSite = '';
var responseStringPromise = '';
var responseStringCase = '';
var responseStringArrayInfo = '';
var responseStringSalesOrder = '';

/* 
This gets us connected to the restlet at netsuite side. 
For testing this can be tried on Postman client. URL would be the hostname + path. Method: POST and headers as written. 
*/
var options = {
    hostname: 'rest.netsuite.com',
    path: '/app/site/hosting/restlet.nl?script=516&deploy=1',
    method: 'POST',
    headers: {
        'Authorization' : 'NLAuth nlauth_account="1292185",nlauth_email="abhardwaj@sunedison.com",nlauth_signature="F9nuvdgetu1?",nlauth_role="1111"',   //Username and Password confidential. 
        'Content-Type': 'application/json',
        'User-Agent': 'SuiteScript-Call'
    }
};

//Connection string to connect to Redshift with username and password. 
var conString = "postgresql://abhardwaj:Master12@sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com:5439/sunedison";
//Query string to insert data into Redshift. 
var queryTextInsertRequest = 'INSERT INTO suned_redshift (suned_cust_id, quote_system_size, quote_ef_cost_per_watt, quote_year1_production, cust_pre_payment, quote_master_lease_pay_esc_rate, quote_rebate, quote_hipbi_year1_value, quote_hipbi_tenure, quote_hipbi_annual_derate, quote_state_tax_rate, quote_current_utility_cost, quote_post_solar_utility_cost, quote_proposal_id, quote_call_version_id, quote_auth_code, system_module_id, system_module_quantity, system_inverter_id, system_inverter_quantity, system_mounting_type, contract_calcmap_current_date, contract_installer_client_name, contract_calcmap_dealer_name, contract_calcmap_howner_0_first_name, contract_calcmap_howner_0_last_name, contract_calcmap_howner_1_first_name, contract_calcmap_howner_1_last_name, contract_product_type, contract_calcmap_n_of_howners, contract_calcmap_howner_0_address, contract_calcmap_howner_0_city, contract_calcmap_howner_0_state, contract_calcmap_howner_0_zipcode, contract_calcmap_howner_0_phone, contract_calcmap_howner_0_email, contract_calcmap_howner_1_address, contract_calcmap_howner_1_city, contract_calcmap_howner_1_state, contract_calcmap_howner_1_zipcode, contract_calcmap_howner_1_phone, contract_calcmap_howner_1_email, contract_calcmap_howner_2_address, contract_calcmap_howner_2_city, contract_calcmap_howner_2_state, contract_calcmap_howner_2_zipcode, contract_calcmap_howner_2_phone, contract_calcmap_howner_2_email, contract_installer_client_phone, contract_production_0_col2, contract_calcmap_lifetime_kwh, array_number, module_type, module_quantity, shading, tilt, azimuth, orientation, monthly_production_values, degradation_rates) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60)';
var queryTextInsertResonse = 'INSERT INTO response (customerleasepayments, pricingquoteid, downpayment, leaseterm, sunedcustid, estimatedannualoutput, uniquefinancialrunid, terminationvalues, suned_timestamp, financialmodelversion, callversionid, guaranteedannualoutput) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)';
//Query string to fetch data from Redshift. 
var queryFetchRequest = 'SELECT * from suned_redshift where suned_cust_id = $1 order by array_number asc';
var queryFetchResponse = 'SELECT * from response where SunEdCustId = $1 and PricingQuoteId = $2';
var queryFetchCustomer = 'SELECT * FROM CUSTOMER ORDER BY LAST_TIMESTAMP DESC';
var queryFetchSite = 'SELECT * FROM SITE ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
var queryFetchPromise = 'SELECT * FROM PROMISE_DATA ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
var queryFetchCase = 'SELECT * FROM CASE_DATA ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
var queryFetchArrayInfo = 'SELECT * FROM ARRAY_INFO ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
var queryFetchContactTest = 'SELECT * FROM TEST_CONTACT ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
var queryTestInsertPromise = 'INSERT INTO PROMISE_DATA (record, promise_type, promise_jan, promise_feb, promise_mar, promise_apr, promise_may, promise_june, promise_july, promise_aug, promise_sept, promise_oct, promise_nov, promise_dec, degradation_rate, expected_to_promise_ratio, last_timestamp, last_modified_date, homeowner_id, site_id) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)';
var queryTestInsertArrayInfo = 'INSERT INTO ARRAY_INFO (record, array_number, thm_module_qty_roof, roof_tilt_in_degrees, orientation, solar_access_jan, solar_access_feb, solar_access_mar, solar_access_apr, solar_access_may, solar_access_june, solar_access_july, solar_access_aug, solar_access_sept, solar_access_oct, solar_access_nov, solar_access_dec, last_timestamp, last_modified_date, homeowner_id, site_id) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)';
var queryTestContactInsertTest = 'INSERT INTO test_contact (record, firstname, lastname, email, last_timestamp, last_modified_date) values ($1,$2,$3,$4,$5,$6)';
var queryResponseColumnList = "SELECT ATTNAME FROM PG_ATTRIBUTE WHERE ATTRELID = 'PUBLIC.RESPONSE'::REGCLASS AND ATTNUM > 0 AND NOT ATTISDROPPED ORDER BY ATTNUM;";
// Main function for AWS Lambda


exports.handler = function(event, context) {

    // Get the object from the event and show its content type
    //var bucket = event.bucketname;
    //var keyRequest = event.keyrequest;
    //var keyResponse = event.keyresponse;
    //accessRequest(bucket, keyRequest);
    //accessResponse(bucket, keyResponse);
    insertNetsuiteCustomerData(); //Getting netsuite customer data to redshift.
    insertNetsuiteSiteData(); //Getting netsuite site data to customer.
    //insertNetsuitePromiseData(); 
    //insertNetsuiteCaseData();
    //insertNetsuiteArrayInfoData();
    //insertNetsuiteContactData();
    insertNetsuiteSalesOrderData();
    //Pulling up inbound payload for Request JSON from AWS S3 bucket. 
}

var accessRequest = function(bucket, keyRequest){
    s3.getObject({Bucket: bucket, Key: keyRequest}, function(err, dataRequest) {
        if (err) {
            console.log("Error getting object " + keyRequest + " from bucket " + bucket +
                ". Make sure they exist and your bucket is in the same region as this function.");
            context.fail ("Error getting file: " + err);      
        } else {
            console.log('CONTENT TYPE: Request : ', dataRequest.ContentType);
            var inbound_payload_request = JSON.parse(dataRequest.Body);
            insertS3RequestData(inbound_payload_request);
        }
    });
}

var accessResponse = function(bucket, keyResponse){
    s3.getObject({Bucket: bucket, Key: keyResponse}, function(err, dataResponse) {
        if (err) {
            console.log("Error getting object " + keyResponse + " from bucket " + bucket +
                ". Make sure they exist and your bucket is in the same region as this function.");
            context.fail ("Error getting file: " + err);      
        } 
        else {
            console.log('CONTENT TYPE: Response : ', dataResponse.ContentType);

            var inbound_payload_response = JSON.parse(dataResponse.Body);
            insertS3ResponseData(inbound_payload_response);
        }
    });
}

var insertS3RequestData = function(inbound_payload_request){
    var SunEdCustId = inbound_payload_request.SunEdCustId;
    var SystemSize = inbound_payload_request.Quote.SystemSize;
    var EFCostPerWatt = inbound_payload_request.Quote.EFCostPerWatt;
    var Year1Production = inbound_payload_request.Quote.Year1Production;
    var CustomerPrepayment = inbound_payload_request.Quote.CustomerPrepayment;
    var MasterLeasePaymentEscalationRate = inbound_payload_request.Quote.MasterLeasePaymentEscalationRate;
    var Rebate = inbound_payload_request.Quote.Rebate;
    var HIPBIYear1Value = inbound_payload_request.Quote.HIPBIYear1Value;
    var HIPBITenure = inbound_payload_request.Quote.HIPBITenure;
    var HIPBIAnnualDerate = inbound_payload_request.Quote.HIPBIAnnualDerate;
    var StateTaxRate = inbound_payload_request.Quote.StateTaxRate;
    var CurrentUtilityCost = inbound_payload_request.Quote.CurrentUtilityCost;
    var PostSolarUtilityCost = inbound_payload_request.Quote.PostSolarUtilityCost;
    var ProposalID = inbound_payload_request.Quote.ProposalID;
    var CallVersionID = inbound_payload_request.Quote.CallVersionID;
    var AuthorizationCode = inbound_payload_request.Quote.AuthorizationCode;
    var ModuleId = inbound_payload_request.System.ModuleId;
    var ModuleQuantity = inbound_payload_request.System.ModuleQuantity;
    var InverterId = inbound_payload_request.System.InverterId;
    var InverterQuantity = inbound_payload_request.System.InverterQuantity;
    var MountingType = inbound_payload_request.System.MountingType;

    var currentDate = inbound_payload_request.Contract["calcMap.currentDate"];
    var installerClientName = inbound_payload_request.Contract["installer.client.name"];
    var dealerName = inbound_payload_request.Contract["calcMap.dealerName"];
    var homeownerList_0_firstName = inbound_payload_request.Contract["calcMap.homeownerList.0.firstName"];
    var homeownerList_0_lastName = inbound_payload_request.Contract["calcMap.homeownerList.0.lastName"];
    var homeownerList_1_firstName = inbound_payload_request.Contract["calcMap.homeownerList.1.firstName"];
    var homeownerList_1_lastName = inbound_payload_request.Contract["calcMap.homeownerList.1.lastName"];
    var product_type = inbound_payload_request.Contract["product_type"];
    var numberOfHomeowners = inbound_payload_request.Contract["calcMap.numberOfHomeowners"];

    var homeownerList_0_address = inbound_payload_request.Contract["calcMap.homeownerList.0.address"];
    var homeownerList_0_city = inbound_payload_request.Contract["calcMap.homeownerList.0.city"];
    var homeownerList_0_state = inbound_payload_request.Contract["calcMap.homeownerList.0.state"];
    var homeownerList_0_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.0.zipCode"];
    var homeownerList_0_phone = inbound_payload_request.Contract["calcMap.homeownerList.0.phone"];
    var homeownerList_0_email = inbound_payload_request.Contract["calcMap.homeownerList.0.email"];

    var homeownerList_1_address = inbound_payload_request.Contract["calcMap.homeownerList.1.address"];
    var homeownerList_1_city = inbound_payload_request.Contract["calcMap.homeownerList.1.city"];
    var homeownerList_1_state = inbound_payload_request.Contract["calcMap.homeownerList.1.state"];
    var homeownerList_1_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.1.zipCode"];
    var homeownerList_1_phone = inbound_payload_request.Contract["calcMap.homeownerList.1.phone"];
    var homeownerList_1_email = inbound_payload_request.Contract["calcMap.homeownerList.1.email"];

    var homeownerList_2_address = inbound_payload_request.Contract["calcMap.homeownerList.2.address"];
    var homeownerList_2_city = inbound_payload_request.Contract["calcMap.homeownerList.2.city"];
    var homeownerList_2_state = inbound_payload_request.Contract["calcMap.homeownerList.2.state"];
    var homeownerList_2_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.2.zipCode"];
    var homeownerList_2_phone = inbound_payload_request.Contract["calcMap.homeownerList.2.phone"];
    var homeownerList_2_email = inbound_payload_request.Contract["calcMap.homeownerList.2.email"];

    var installerClientPhone = inbound_payload_request.Contract["installer.client.phone"];
    var productionList_0_col2 = inbound_payload_request.Contract["productionList.0.col2"];
    var lifeTimeKwh = inbound_payload_request.Contract["calcMap.lifetimekWh"];  

    var suned_id = parseInt(SunEdCustId, 10);

    pg.connect(conString, function(err,client){
        if(err){
            return console.log("Connection error. ", err);
        }

        console.log("Connection Established under fetch");

        //Querying redshift. 
        client.query(queryFetchRequest, [SunEdCustId], function(err,result){
            if(err){
                console.log("Error returning query", err);
                context.done("Fatal Error");
            }
            console.log("Number of rows: ", result.rows.length);
            console.log("Number of rows from JSON: " + inbound_payload_request.Array.length);

            //Algorithm to check redundancy and add unique data into redshift. 
            for(var m=0;m<inbound_payload_request.Array.length;m++){
                
                //Insert all the data from JSON file if no data exists in Redshift. 
                if(result.rows.length == 0){
                    console.log("No records in Redshift");
                    insertIntoRedshiftRequest(m);
                }

                //Check for duplicacy and insert rows to redshift. 
                else{
                    for(var k=0;k<result.rows.length;k++){
                        if(result.rows[k].suned_cust_id == SunEdCustId && result.rows[k].array_number == inbound_payload_request.Array[m].ArrayNumber){
                            console.log("Duplicate Row Exists.");
                            break;           
                        }
                        else if(k == result.rows.length-1){
                            insertIntoRedshiftRequest(m);
                        } 
                    }
                }       
            }
        });
    });

}

var insertS3ResponseData = function(inbound_payload_response){
    var SunEdCustIdResponse = inbound_payload_response.SunEdCustId;
    var PricingQuoteId = inbound_payload_response.PricingQuoteId;
    var customerLeasePaymentsArray = [];
    var DownPayment = inbound_payload_response.DownPayment;
    var LeaseTerm = inbound_payload_response.LeaseTerm; 
    var estimatedAnnualOutputArray = [];
    var EstimatedAnnualOutput = inbound_payload_response.EstimatedAnnualOutput;
    var UniqueFinancialRunId = inbound_payload_response.UniqueFinancialRunId;
    var terminationValuesArray = [];
    var Suned_Timestamp = inbound_payload_response.Timestamp;
    var FinancialModelVersion = inbound_payload_response.FinancialModelVersion;
    var CallVersionId = inbound_payload_response.CallVersionId; 
    var GuaranteedAnnualOutput = inbound_payload_response.GuaranteedAnnualOutput;   

    for(var i=0;i<LeaseTerm;i++){
        customerLeasePaymentsArray.push(inbound_payload_response.CustomerLeasePayments[i]);
    }

    for(var j=0;j<LeaseTerm;j++){
        terminationValuesArray.push(inbound_payload_response.TerminationValues[j]);
    } 

    var NewEstimatedAnnualOutput = EstimatedAnnualOutput.substring(1,EstimatedAnnualOutput.length-1);
    var NewGuaranteedAnnualOutput = GuaranteedAnnualOutput.substring(1,GuaranteedAnnualOutput.length-1);

    pg.connect(conString, function(err,client){
        if(err){
            return console.log("Connection error. ", err);
        }

        console.log("Connection Established under fetch");

        //Querying redshift. 
        client.query(queryFetchResponse, [SunEdCustIdResponse,PricingQuoteId], function(err,resultFirst){
            if(err){
                console.log("Error returning query", err);
                context.done("Fatal Error");
            }
            console.log("Number of rows: ", resultFirst.rows.length);
            console.log("Number of Arrays for CustomerLeasePayments in JSON: " + inbound_payload_response.CustomerLeasePayments.length);

            if(resultFirst.rows.length == 0){
                insertIntoRedshiftResponse();
            }
            else{
                client.query(queryResponseColumnList, function(err,resultMid){
                    if(err){
                        console.log("Error returning query", err);
                        context.done("Fatal Error");
                    }
                    for(var z=0; z<resultMid.rows.length; z++){
                        for(var attributename in inbound_payload_response){
                            //console.log("Attribute names from DB: " + resultMid.rows[z].attname);
                            //console.log("Check: " + resultFirst.rows[0].sunedcustid);
                            var fieldValue = resultMid.rows[z].attname;
                            var dbCustId = resultFirst.rows[0].sunedcustid;
                            var queryFetchColumnDataForAttribute = 'SELECT ' + fieldValue +' as fieldvalue from response where SunEdCustId='+ dbCustId;

                            client.query(queryFetchColumnDataForAttribute, function(err,resultLast){
                                if(err){
                                    console.log(err);
                                }

                                //if(resultLast.rows[0].fieldvalue == inbound_payload_response[attributename])
                                //console.log("---- See here for result -----");
                                //console.log(resultLast.rows[0].fieldvalue);
                                if(resultLast.rows[0].fieldvalue != inbound_payload_response[attributename]){
                                    console.log("Value changed of: " + fieldValue);
                                }
                                else{
                                    console.log("Something wrong. CHECK !!");
                                }
                            });
                            return;
                        }
                        
                    }
                });
                console.log("Data already exisits for received customer ID.");
            }
        
        });
    });
}

var insertIntoRedshiftRequest = function(m){
    pg.connect(conString, function(err,client){
        if(err){
            return console.log("Connection Error.", err);
        }
        console.log("Connection Established.");
        client.query(queryTextInsertRequest, [suned_id, SystemSize, EFCostPerWatt, Year1Production, CustomerPrepayment, MasterLeasePaymentEscalationRate, Rebate, HIPBIYear1Value, HIPBITenure, HIPBIAnnualDerate, StateTaxRate, CurrentUtilityCost, PostSolarUtilityCost, ProposalID, CallVersionID, AuthorizationCode, ModuleId, ModuleQuantity, InverterId, InverterQuantity, MountingType, currentDate, installerClientName, dealerName, homeownerList_0_firstName, homeownerList_0_lastName, homeownerList_1_firstName, homeownerList_1_lastName, product_type, numberOfHomeowners, homeownerList_0_address, homeownerList_0_city, homeownerList_0_state, homeownerList_0_zipcode, homeownerList_0_phone, homeownerList_0_email, homeownerList_1_address, homeownerList_1_city, homeownerList_1_state, homeownerList_1_zipcode, homeownerList_1_phone, homeownerList_1_email, homeownerList_2_address,homeownerList_2_city, homeownerList_2_state, homeownerList_2_zipcode, homeownerList_2_phone, homeownerList_2_email, installerClientPhone, productionList_0_col2, lifeTimeKwh, inbound_payload_request.Array[m].ArrayNumber, inbound_payload_request.Array[m].ModuleType, inbound_payload_request.Array[m].ModuleQuantity, inbound_payload_request.Array[m].Shading, inbound_payload_request.Array[m].Tilt, inbound_payload_request.Array[m].Azimuth, inbound_payload_request.Array[m].Orientation, inbound_payload_request.Array[m].monthlyProductionValues, inbound_payload_request.Array[m].DegradationRate], function(err,result){
            if(err){
                return console.log('Error returning query', err);
            }
            console.log('Row inserted from Request JSON. Go and check on Redshift: ' + result);
            return client;
        });
        
    }); 
}

var insertIntoRedshiftResponse = function(){
    pg.connect(conString, function(err,client){
        if(err){
            return console.log("Connection Error.", err);
        }
        console.log("Connection Established.");
        client.query(queryTextInsertResonse, [customerLeasePaymentsArray.toString(), PricingQuoteId, DownPayment, LeaseTerm, SunEdCustIdResponse, NewEstimatedAnnualOutput, UniqueFinancialRunId, terminationValuesArray.toString(), Suned_Timestamp, FinancialModelVersion, CallVersionId, NewGuaranteedAnnualOutput], function(err,result){
            if(err){
                return console.log('Error returning query', err);
            }
            console.log('Row inserted from Response JSON. Go and check on Redshift: ' + result);
            return client;
            end();
            pg.end();  
        });   
    });
} 

var insertNetsuiteCustomerData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchCustomer, function(err,result){
            if(err){
                console.log("Error returning query", err);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            var uniqueRecordId = result.rows[0].record;
            console.log("Last known timestamp " + lastModifiedDate);

            var postData = {recordtype:'customer',recordId:uniqueRecordId,timestamp:lastModifiedDate};
            var postString = JSON.stringify(postData);
            var req = http.request(options, function(res) {

                console.log("This goes to netsuite" + postString);
                res.setEncoding('utf-8');
                
                console.log("Received response: " + res.statusCode);
                res.on("data",function(data){
                    responseString += data;
                });

                res.on('end', function() {
                    var resultObject = JSON.parse(responseString);
                    for(var i=0;i<resultObject.length;i++){
                        if(resultObject[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBCustomer(i, resultObject);      
                        }
                        
                    }
                });
            });
            req.on('error', function(e) {
                console.log("Error" + e);
            });
                
            req.write(postString);
            req.end();

        });

        var fetchDuplicateRowsFromDBCustomer = function(i, resultObject){
            var recordId = resultObject[i].recordId;
            var checkDuplicateRecordQuery = "SELECT * from customer where record = '" + recordId + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuiteCustomerData(i,resultObject);
                }
                else{
                    updateIntoRedshiftNetsuiteCustomerData(i, recordId, "firstname","lastname","email","fully_executed_date","contract_canceled_date","homeowner_proj_document_sta_id","proposal_status_id","purchase_type_id","partner_sub_type_id","homeowner_lease_contract_st_id","assigned_partner_id","proposed_system_size_pvkwh","credit_check_last_update_date","lease_contract_status_last_up","proj_definition_doc_last_upda","executed_assign_agrmnt_updat","last_timestamp","last_modified_date","site_id","financing_program","date_created","proposal_status_last_update_date","homeowner_credit_check_status","executed_assgn_agmnt_status","subsidiary_no_hierarchy","pda_signed_date","lead_lost","sales_agent","assigned_to_partner_sales_agent","total_system_size_echo_watts","homephone", "service_panel_upgrade","is_homeowner", resultObject);
                }
            });
        }  

        var insertIntoRedshiftNetsuiteCustomerData = function(i,resultObject){
            if(resultObject[i].Fully_Executed_Date == '')
                var temp_Fully_Executed_Date = null;
            else
                var temp_Fully_Executed_Date = "'" + resultObject[i].Fully_Executed_Date + "'";

            if(resultObject[i].custentity_contract_canceled_date == '')
                var temp_custentity_contract_canceled_date = null;
            else
                var temp_custentity_contract_canceled_date = "'" + resultObject[i].custentity_contract_canceled_date + "'";

            if(resultObject[i].custentity208 == '')
                var temp_custentity208 = null;
            else{
                var temp_custentity208 = "'" + resultObject[i].custentity208 + "'";
            }

            if(resultObject[i].custentity209 == '')
                var temp_custentity209 = null;
            else
                var temp_custentity209 = "'" + resultObject[i].custentity209 + "'";

            if(resultObject[i].custentity21 == '')
                var temp_custentity21 = null;
            else
                var temp_custentity21 = "'" + resultObject[i].custentity21 + "'";

            if(resultObject[i].custentity_executed_assign_agrmnt_date == '')
                var temp_custentity_executed_assign_agrmnt_date = null;
            else
                var temp_custentity_executed_assign_agrmnt_date = "'" + resultObject[i].custentity_executed_assign_agrmnt_date + "'";

            if(resultObject[i].datecreated == '')
                var temp_datecreated = null;
            else
                var temp_datecreated = "'" + resultObject[i].datecreated + "'";

            if(resultObject[i].custentity_proposaldate == '')
                var temp_proposaldate = null;
            else
                var temp_proposaldate = "'" + resultObject[i].custentity_proposaldate + "'";

            if(resultObject[i].custentity_nuvola_pda_signeddate == '')
                var temp_pda_signeddate = null;
            else
                var temp_pda_signeddate = "'" + resultObject[i].custentity_nuvola_pda_signeddate + "'";

            var queryTestInsertCustomer = "INSERT INTO CUSTOMER (record, firstname, lastname, email, fully_executed_date, contract_canceled_date, homeowner_proj_document_sta_id, proposal_status_id, purchase_type_id, partner_sub_type_id, homeowner_lease_contract_st_id, assigned_partner_id, proposed_system_size_pvkwh, credit_check_last_update_date, lease_contract_status_last_up, proj_definition_doc_last_upda, executed_assign_agrmnt_updat, last_timestamp, last_modified_date, site_id, financing_program, date_created, proposal_status_last_update_date, homeowner_credit_check_status, executed_assgn_agmnt_status, subsidiary_no_hierarchy, pda_signed_date, lead_lost, sales_agent, assigned_to_partner_sales_agent, total_system_size_echo_watts, homephone, service_panel_upgrade, is_homeowner) values ('" + resultObject[i].recordId.replace("'","''") + "','" + resultObject[i].firstname.replace("'","''") + "','" + resultObject[i].lastname.replace("'","''") + "','" + resultObject[i].email.replace("'","''") + "'," + temp_Fully_Executed_Date + "," + temp_custentity_contract_canceled_date + ",'" + resultObject[i].custentity206.replace("'","''") + "','" + resultObject[i].custentity_proposalstatus.replace("'","''") + "','" + resultObject[i].custentity200.replace("'","''") + "','" + resultObject[i].custentity_dsg_partner_sub_type.replace("'","''") + "','" + resultObject[i].custentity205.replace("'","''") + "','" + resultObject[i].custentity_assign_partner.replace("'","''") + "','" + resultObject[i].custentity239.replace("'","''") + "'," + temp_custentity208 + "," + temp_custentity209 + "," + temp_custentity21 + ","+ temp_custentity_executed_assign_agrmnt_date + ",'" + resultObject[i].lastModifiedDate + "','" + resultObject[i].lastModifiedDate + "','" + resultObject[i].site_id.replace("'","''") + "','" + resultObject[i].custentity387.replace("'","''") + "'," + temp_datecreated + "," + temp_proposaldate + ",'" + resultObject[i].custentity204.replace("'","''") + "','" + resultObject[i].custentity_executed_assign_agmnt_status.replace("'","''") + "','" + resultObject[i].subsidiarynohierarchy.replace("'","''") + "'," + temp_pda_signeddate + ",'" + resultObject[i].custentity323.replace("'","''") + "','" + resultObject[i].custentity_sales_agent.replace("'","''") + "','" + resultObject[i].custentity214.replace("'","''") + "','" + resultObject[i].custentity245.replace("'","''") + "','" + resultObject[i].homephone.replace("'","''") + "','" + resultObject[i].custentity_dsg_service_panel_upgrade.replace("'","''") + "','" + resultObject[i].is_homeowner.replace("'","''") + "')";
            client.query(queryTestInsertCustomer, function(err,result){
                if(err){
                    return console.log('Error returning query: (Customer Insert)' + err + queryTestInsertCustomer);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(Customer): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuiteCustomerData = function(i, recordId, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6,fieldName7,fieldName8,fieldName9,fieldName10,fieldName11,fieldName12,fieldName13,fieldName14,fieldName15,fieldName16,fieldName17,fieldName18,fieldName19,fieldName20,fieldName21,fieldName22,fieldName23,fieldName24,fieldName25,fieldName26,fieldName27,fieldName28,fieldName29,fieldName30,fieldName31,fieldName32,fieldName33, resultObject){
            if(resultObject[i].Fully_Executed_Date == '')
                var temp_Fully_Executed_Date = null;
            else
                var temp_Fully_Executed_Date = "'" + resultObject[i].Fully_Executed_Date + "'";

            if(resultObject[i].custentity_contract_canceled_date == '')
                var temp_custentity_contract_canceled_date = null;
            else
                var temp_custentity_contract_canceled_date = "'" + resultObject[i].custentity_contract_canceled_date + "'";

            if(resultObject[i].custentity208 == '')
                var temp_custentity208 = null;
            else{
                var temp_custentity208 = "'" + resultObject[i].custentity208 + "'";
            }

            if(resultObject[i].custentity209 == '')
                var temp_custentity209 = null;
            else
                var temp_custentity209 = "'" + resultObject[i].custentity209 + "'";

            if(resultObject[i].custentity21 == '')
                var temp_custentity21 = null;
            else
                var temp_custentity21 = "'" + resultObject[i].custentity21 + "'";

            if(resultObject[i].custentity_executed_assign_agrmnt_date == '')
                var temp_custentity_executed_assign_agrmnt_date = null;
            else
                var temp_custentity_executed_assign_agrmnt_date = "'" + resultObject[i].custentity_executed_assign_agrmnt_date + "'";

            if(resultObject[i].datecreated == '')
                var temp_datecreated = null;
            else
                var temp_datecreated = "'" + resultObject[i].datecreated + "'";

            if(resultObject[i].custentity_proposaldate == '')
                var temp_proposaldate = null;
            else
                var temp_proposaldate = "'" + resultObject[i].custentity_proposaldate + "'";

            if(resultObject[i].custentity_nuvola_pda_signeddate == '')
                var temp_pda_signeddate = null;
            else
                var temp_pda_signeddate = "'" + resultObject[i].custentity_nuvola_pda_signeddate + "'";

            var queryUpdateCustomer = "UPDATE customer SET " + fieldName1 + " = '" + resultObject[i].firstname.replace("'","''") + "', " + fieldName2 + " = '" + resultObject[i].lastname.replace("'","''") + "', " + fieldName3 + " = '" + resultObject[i].email.replace("'","''") + "'," + fieldName4 + " = " + temp_Fully_Executed_Date + ", " + fieldName5 + " = " + temp_custentity_contract_canceled_date + ", " + fieldName6 + " = '" + resultObject[i].custentity206.replace("'","''") + "', " + fieldName7 + " = '" + resultObject[i].custentity_proposalstatus.replace("'","''") + "', " + fieldName8 + " = '" + resultObject[i].custentity200.replace("'","''") + "', " + fieldName9 + " = '" + resultObject[i].custentity_dsg_partner_sub_type.replace("'","''") + "', " + fieldName10 + " = '" + resultObject[i].custentity205.replace("'","''") + "', " + fieldName11 + " = '" + resultObject[i].custentity_assign_partner.replace("'","''") + "', " + fieldName12 + " = '" + resultObject[i].custentity239.replace("'","''") + "', " + fieldName13 + " = " + temp_custentity208 + ", " + fieldName14 + " = " + temp_custentity209 + ", " + fieldName15 + " = " + temp_custentity21 + ", " + fieldName16 + " = " + temp_custentity_executed_assign_agrmnt_date + ", " + fieldName17 + " = '" + resultObject[i].lastModifiedDate + "', " + fieldName18 + " = '" + resultObject[i].lastModifiedDate + "', " + fieldName19 + " = '" + resultObject[i].site_id.replace("'","''") + "', " + fieldName20 + " = '" + resultObject[i].custentity387.replace("'","''") + "', " + fieldName21 + " = " + temp_datecreated + ", " + fieldName22 + " = " + temp_proposaldate + ", " + fieldName23 + " = '" + resultObject[i].custentity204.replace("'","''") + "', " + fieldName24 + " = '" + resultObject[i].custentity_executed_assign_agmnt_status.replace("'","''") + "', " + fieldName25 + " = '" + resultObject[i].subsidiarynohierarchy.replace("'","''") + "', " + fieldName26 + " = " + temp_pda_signeddate + ", " + fieldName27 + " = '" + resultObject[i].custentity323.replace("'","''") + "', " + fieldName28 + " = '" + resultObject[i].custentity_sales_agent.replace("'","''") + "', " + fieldName29 + " = '" + resultObject[i].custentity214.replace("'","''") + "', " + fieldName30 + " = '" + resultObject[i].custentity245.replace("'","''") + "', " + fieldName31 + " = '" + resultObject[i].homephone.replace("'","''") + "', " + fieldName32 + " = '" + resultObject[i].custentity_dsg_service_panel_upgrade.replace("'","''") + "', " + fieldName33 + " = '" + resultObject[i].is_homeowner.replace("'","''") + "' WHERE record = '" + recordId + "'";
            //console.log(queryUpdateCustomer);
            client.query(queryUpdateCustomer, function(err,result){
                if(err){
                    return console.log('Error returning query: (Customer Update)' + err + queryUpdateCustomer);
                }
                console.log('Updated Customer.');
                return client;
            });
        }
    });
}

var insertNetsuiteSiteData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchSite, function(error,result){
            if(error){
                console.log("Error returning query", error);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            var uniqueRecordId = result.rows[0].record;
            console.log("Last known timestamp " + lastModifiedDate);

            var postDataSite = {recordtype:'site',recordId:uniqueRecordId,timestamp:lastModifiedDate};
            var postStringSite = JSON.stringify(postDataSite);
            var reqSite = http.request(options, function(response) {

                console.log("This goes to netsuite" + postStringSite);
                response.setEncoding('utf-8');
                
                console.log("Received response(site): " + response.statusCode);
                response.on("data",function(dataSite){
                    responseStringSite += dataSite;
                });

                response.on('end', function() {
                    var resultObjectSite = JSON.parse(responseStringSite);
                    for(var i=0;i<resultObjectSite.length;i++){
                        var recordId = resultObjectSite[i].recordId;
                        if(resultObjectSite[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBSite(i, resultObjectSite);
                        }   
                    }
                });
                
            });
            reqSite.on('error', function(e) {
                console.log("Error" + e);
            });
                
            reqSite.write(postStringSite);
            reqSite.end();

        });
        
        var fetchDuplicateRowsFromDBSite = function(i, resultObjectSite){
            var recordId = resultObjectSite[i].recordId;
            var checkDuplicateRecordQuery = "SELECT * from site where record = '" + recordId + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                //console.log("Record found: " + resultLast.rows.length);
                //console.log(checkDuplicateRecordQuery);
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuiteSiteData(i,resultObjectSite);
                }
                else{
                    updateIntoRedshiftNetsuiteSiteData(i, recordId, "site_id","homeowner_id","milestone_3_payment_approval_","site_visit_status_id","tranche_id","site_visit_date","installer2_payment_approval_d","milestone_2_payment_approval_","installer3_payment_approval_d","partner_id","sai_territory_id","commissioned_date","street_1","city","state_id","zip","pv_module_model","pv_modules_quantity","inverter_mfr_1_id","inverter_model_1_id","inverter_quantity","final_completion_certificate","controller_id","product_type","permission_to_operate_date","system_operation_verified","is_homeowner","inverter_model_2","inverter_model_3","inverter_mfr_2","inverter_mfr_3","enphase_envoy_id","pv_module_mfg","pv_module_size_in_watts","monitoring_model","monitoring_type","last_timestamp","last_modified_date","tsm","sales_channel_id","sai_installer","salesorder_to_homeowner_id","nr_interconnected_so","total_contract_price","install_status","permit_status","actual_permit_date","cpm_list","site_survey_scheduled_date","site_survey_date_change_reason","permit_submit_date","scheduled_install_date","target_permit_date","install_completed_date","ahj_inspection_status","ahj_inspection_approval_date","installer2_payment_status","ms2_payment_status","doc_processing_status13","submittal_date13","doc_processing_status14","installer3_payment_status","ms3_payment_status","lien_doc_processing_status","se2_payment_approval_date","homeowner","target_install_date","date_created", resultObjectSite);
                }
            });
        }
        
        var insertIntoRedshiftNetsuiteSiteData = function(i,resultObjectSite){
            if(resultObjectSite[i].custrecord_dsg_milestone3_pay_appr_date == '')
                var temp_custrecord_dsg_milestone3_pay_appr_date = null;
            else
                var temp_custrecord_dsg_milestone3_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_milestone3_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_site_visit_date == '')
                var temp_custrecord_dsg_site_visit_date = null;
            else
                var temp_custrecord_dsg_site_visit_date = "'" + resultObjectSite[i].custrecord_dsg_site_visit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_milestone2_pay_appr_date == '')
                var temp_custrecord_dsg_milestone2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_milestone2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_milestone2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_inst2_pay_appr_date == '')
                var temp_custrecord_dsg_inst2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_inst2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_inst2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_inst3_pay_appr_date == '')
                var temp_custrecord_dsg_inst3_pay_appr_date = null;
            else
                var temp_custrecord_dsg_inst3_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_inst3_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_comm_date == '')
                var temp_custrecord_dsg_comm_date = null;
            else
                var temp_custrecord_dsg_comm_date = "'" + resultObjectSite[i].custrecord_dsg_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_utility_interconnex_date == '')
                var temp_custrecord_dsg_utility_interconnex_date = null;
            else
                var temp_custrecord_dsg_utility_interconnex_date = "'" + resultObjectSite[i].custrecord_dsg_utility_interconnex_date + "'";

            if(resultObjectSite[i].custrecord_dsg_sys_operation_verify == '')
                var temp_custrecord_dsg_sys_operation_verify = null;
            else
                var temp_custrecord_dsg_sys_operation_verify = "'" + resultObjectSite[i].custrecord_dsg_sys_operation_verify + "'";

            if(resultObjectSite[i].custrecord_dsg_actual_permit_date == '')
                var temp_custrecord_dsg_actual_permit_date = null;
            else
                var temp_custrecord_dsg_actual_permit_date = "'" + resultObjectSite[i].custrecord_dsg_actual_permit_date + "'";

            if(resultObjectSite[i].custrecord_se_site_visit_sch_date == '')
                var temp_custrecord_se_site_visit_sch_date = null;
            else
                var temp_custrecord_se_site_visit_sch_date = "'" + resultObjectSite[i].custrecord_se_site_visit_sch_date + "'";

            if(resultObjectSite[i].custrecord_se_permit_submit_date == '')
                var temp_custrecord_se_permit_submit_date = null;
            else
                var temp_custrecord_se_permit_submit_date = "'" + resultObjectSite[i].custrecord_se_permit_submit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_install_comm_date == '')
                var temp_custrecord_dsg_target_install_comm_date = null;
            else
                var temp_custrecord_dsg_target_install_comm_date = "'" + resultObjectSite[i].custrecord_dsg_target_install_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_permit_date == '')
                var temp_custrecord_dsg_target_permit_date = null;
            else
                var temp_custrecord_dsg_target_permit_date = "'" + resultObjectSite[i].custrecord_dsg_target_permit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_act_install_comm_date == '')
                var temp_custrecord_dsg_act_install_comm_date = null;
            else
                var temp_custrecord_dsg_act_install_comm_date = "'" + resultObjectSite[i].custrecord_dsg_act_install_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_ahj_inspect_approval_date == '')
                var temp_custrecord_dsg_ahj_inspect_approval_date = null;
            else
                var temp_custrecord_dsg_ahj_inspect_approval_date = "'" + resultObjectSite[i].custrecord_dsg_ahj_inspect_approval_date + "'";

            if(resultObjectSite[i].custrecord_dsg_m3_dt3 == '')
                var temp_custrecord_dsg_m3_dt3 = null;
            else
                var temp_custrecord_dsg_m3_dt3 = "'" + resultObjectSite[i].custrecord_dsg_m3_dt3 + "'";

            if(resultObjectSite[i].custrecord_dsg_se2_pay_appr_date == '')
                var temp_custrecord_dsg_se2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_se2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_se2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_install_date == '')
                var temp_custrecord_dsg_target_install_date = null;
            else
                var temp_custrecord_dsg_target_install_date = "'" + resultObjectSite[i].custrecord_dsg_target_install_date + "'";

            if(resultObjectSite[i].date_created == '')
                var temp_date_created = null;
            else
                var temp_date_created = "'" + resultObjectSite[i].date_created + "'";

            var queryTestInsertSite = "INSERT INTO SITE (record, site_id, homeowner_id, milestone_3_payment_approval_, site_visit_status_id, tranche_id, site_visit_date, installer2_payment_approval_d, milestone_2_payment_approval_, installer3_payment_approval_d, partner_id, sai_territory_id, commissioned_date, street_1, city, state_id, zip, pv_module_model, pv_modules_quantity, inverter_mfr_1_id, inverter_model_1_id, inverter_quantity, final_completion_certificate, controller_id, product_type, permission_to_operate_date, system_operation_verified, is_homeowner, inverter_model_2, inverter_model_3, inverter_mfr_2, inverter_mfr_3, enphase_envoy_id, pv_module_mfg, pv_module_size_in_watts, monitoring_model, monitoring_type, last_timestamp, last_modified_date, tsm, sales_channel_id, sai_installer, salesorder_to_homeowner_id,nr_interconnected_so,total_contract_price,install_status,permit_status,actual_permit_date,cpm_list,site_survey_scheduled_date,site_survey_date_change_reason,permit_submit_date,scheduled_install_date,target_permit_date,install_completed_date,ahj_inspection_status,ahj_inspection_approval_date,installer2_payment_status,ms2_payment_status,doc_processing_status13,submittal_date13,doc_processing_status14,installer3_payment_status,ms3_payment_status,lien_doc_processing_status,se2_payment_approval_date,homeowner,target_install_date,date_created) values ('" + resultObjectSite[i].recordId.replace("'","''") + "','" + resultObjectSite[i].site_id.replace("'","''") + "','" + resultObjectSite[i].site_homeowner_Id.replace("'","''") + "'," + temp_custrecord_dsg_milestone3_pay_appr_date + ",'" + resultObjectSite[i].custrecord_dsg_site_visit_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_tranche_id.replace("'","''") + "'," + temp_custrecord_dsg_site_visit_date + "," + temp_custrecord_dsg_inst2_pay_appr_date + "," + temp_custrecord_dsg_milestone2_pay_appr_date + "," + temp_custrecord_dsg_inst3_pay_appr_date + ",'" + resultObjectSite[i].custrecord_dsg_partner.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_territory_custom.replace("'","''") + "'," + temp_custrecord_dsg_comm_date + ",'" + resultObjectSite[i].custrecord_dsg_site_street_1.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_site_city.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_site_state.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_site_zip.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_pv_module_model.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_pv_modules_qty.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_mfr_1.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_model_1.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_qty.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_m3_doc7.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_controller_id.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_product_type.replace("'","''") + "'," + temp_custrecord_dsg_utility_interconnex_date + "," + temp_custrecord_dsg_sys_operation_verify + ",'" + resultObjectSite[i].custrecord_dsg_is_homeowner.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_model_2.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_model_3.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_mfr_2.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inverter_mfr_3.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_enphase_envoy_id.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_pv_module_mfg.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_pv_module_size.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_monitoring_model.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_monitoring_type.replace("'","''") + "','" + resultObjectSite[i].lastModifiedDate + "','" + resultObjectSite[i].lastModifiedDate + "','" + resultObjectSite[i].custrecord_nuvola_tsm.replace("'","''") + "','" + resultObjectSite[i].custrecord_sales_channel.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_site_scion_installer.replace("'","''") + "','" + resultObjectSite[i].custrecord_se_nr_so_to_ho_or_id.replace("'","''") + "','" + resultObjectSite[i].custrecord_se_nr_interconnected_so.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_tot_contract_amt.replace("'","''") + "','" + resultObjectSite[i].custrecord_se_install_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_se_permitstatus.replace("'","''") + "'," + temp_custrecord_dsg_actual_permit_date + ",'" + resultObjectSite[i].custrecord_dsg_cpm_list.replace("'","''") + "'," + temp_custrecord_se_site_visit_sch_date + ",'" + resultObjectSite[i].custrecord_dsg_site_vst_date_chng_reason.replace("'","''") + "'," + temp_custrecord_se_permit_submit_date + "," + temp_custrecord_dsg_target_install_comm_date + "," + temp_custrecord_dsg_target_permit_date + "," + temp_custrecord_dsg_act_install_comm_date + ",'" + resultObjectSite[i].custrecord_ahj_inspection_status.replace("'","''") + "'," + temp_custrecord_dsg_ahj_inspect_approval_date + ",'" + resultObjectSite[i].custrecord_dsg_inst2_pmt_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_m2_pmt_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_m3_stat3.replace("'","''") + "'," + temp_custrecord_dsg_m3_dt3 + ",'" + resultObjectSite[i].custrecord_dsg_m3_stat4.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_inst3_pmt_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_m3_pmt_status.replace("'","''") + "','" + resultObjectSite[i].custrecord_dsg_lien_doc_status.replace("'","''") + "'," + temp_custrecord_dsg_se2_pay_appr_date + ",'" + resultObjectSite[i].site_homeowner_Name.replace("'","''") + "'," + temp_custrecord_dsg_target_install_date + "," + temp_date_created + ")";
            client.query(queryTestInsertSite, function(err,result){
                //console.log(queryTestInsertSite);
                if(err){
                    return console.log('Error returning query (Insert Site)' + err + queryTestInsertSite);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(Site): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuiteSiteData = function(i, recordId, site_id, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6,fieldName7,fieldName8,fieldName9,fieldName10,fieldName11,fieldName12,fieldName13,fieldName14,fieldName15,fieldName16,fieldName17,fieldName18,fieldName19,fieldName20,fieldName21,fieldName22,fieldName23,fieldName24,fieldName25,fieldName26,fieldName27,fieldName28,fieldName29,fieldName30,fieldName31,fieldName32,fieldName33,fieldName34,fieldName35,fieldName36,fieldName37,fieldName38,fieldName39,fieldName40,fieldName41,fieldName42,fieldName43,fieldName44,fieldName45,fieldName46,fieldName47,fieldName48,fieldName49,fieldName50,fieldName51,fieldName52,fieldName53,fieldName54,fieldName55,fieldName56,fieldName57,fieldName58,fieldName59,fieldName60,fieldName61,fieldName62,fieldName63,fieldName64,fieldName65,fieldName66,fieldName67, resultObjectSite){
            if(resultObjectSite[i].custrecord_dsg_milestone3_pay_appr_date == '')
                var temp_custrecord_dsg_milestone3_pay_appr_date = null;
            else
                var temp_custrecord_dsg_milestone3_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_milestone3_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_site_visit_date == '')
                var temp_custrecord_dsg_site_visit_date = null;
            else
                var temp_custrecord_dsg_site_visit_date = "'" + resultObjectSite[i].custrecord_dsg_site_visit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_milestone2_pay_appr_date == '')
                var temp_custrecord_dsg_milestone2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_milestone2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_milestone2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_inst2_pay_appr_date == '')
                var temp_custrecord_dsg_inst2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_inst2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_inst2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_inst3_pay_appr_date == '')
                var temp_custrecord_dsg_inst3_pay_appr_date = null;
            else
                var temp_custrecord_dsg_inst3_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_inst3_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_comm_date == '')
                var temp_custrecord_dsg_comm_date = null;
            else
                var temp_custrecord_dsg_comm_date = "'" + resultObjectSite[i].custrecord_dsg_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_utility_interconnex_date == '')
                var temp_custrecord_dsg_utility_interconnex_date = null;
            else
                var temp_custrecord_dsg_utility_interconnex_date = "'" + resultObjectSite[i].custrecord_dsg_utility_interconnex_date + "'";

            if(resultObjectSite[i].custrecord_dsg_sys_operation_verify == '')
                var temp_custrecord_dsg_sys_operation_verify = null;
            else
                var temp_custrecord_dsg_sys_operation_verify = "'" + resultObjectSite[i].custrecord_dsg_sys_operation_verify + "'";

            if(resultObjectSite[i].custrecord_dsg_actual_permit_date == '')
                var temp_custrecord_dsg_actual_permit_date = null;
            else
                var temp_custrecord_dsg_actual_permit_date = "'" + resultObjectSite[i].custrecord_dsg_actual_permit_date + "'";

            if(resultObjectSite[i].custrecord_se_site_visit_sch_date == '')
                var temp_custrecord_se_site_visit_sch_date = null;
            else
                var temp_custrecord_se_site_visit_sch_date = "'" + resultObjectSite[i].custrecord_se_site_visit_sch_date + "'";

            if(resultObjectSite[i].custrecord_se_permit_submit_date == '')
                var temp_custrecord_se_permit_submit_date = null;
            else
                var temp_custrecord_se_permit_submit_date = "'" + resultObjectSite[i].custrecord_se_permit_submit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_install_comm_date == '')
                var temp_custrecord_dsg_target_install_comm_date = null;
            else
                var temp_custrecord_dsg_target_install_comm_date = "'" + resultObjectSite[i].custrecord_dsg_target_install_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_permit_date == '')
                var temp_custrecord_dsg_target_permit_date = null;
            else
                var temp_custrecord_dsg_target_permit_date = "'" + resultObjectSite[i].custrecord_dsg_target_permit_date + "'";

            if(resultObjectSite[i].custrecord_dsg_act_install_comm_date == '')
                var temp_custrecord_dsg_act_install_comm_date = null;
            else
                var temp_custrecord_dsg_act_install_comm_date = "'" + resultObjectSite[i].custrecord_dsg_act_install_comm_date + "'";

            if(resultObjectSite[i].custrecord_dsg_ahj_inspect_approval_date == '')
                var temp_custrecord_dsg_ahj_inspect_approval_date = null;
            else
                var temp_custrecord_dsg_ahj_inspect_approval_date = "'" + resultObjectSite[i].custrecord_dsg_ahj_inspect_approval_date + "'";

            if(resultObjectSite[i].custrecord_dsg_m3_dt3 == '')
                var temp_custrecord_dsg_m3_dt3 = null;
            else
                var temp_custrecord_dsg_m3_dt3 = "'" + resultObjectSite[i].custrecord_dsg_m3_dt3 + "'";

            if(resultObjectSite[i].custrecord_dsg_se2_pay_appr_date == '')
                var temp_custrecord_dsg_se2_pay_appr_date = null;
            else
                var temp_custrecord_dsg_se2_pay_appr_date = "'" + resultObjectSite[i].custrecord_dsg_se2_pay_appr_date + "'";

            if(resultObjectSite[i].custrecord_dsg_target_install_date == '')
                var temp_custrecord_dsg_target_install_date = null;
            else
                var temp_custrecord_dsg_target_install_date = "'" + resultObjectSite[i].custrecord_dsg_target_install_date + "'";   

            if(resultObjectSite[i].date_created == '')
                var temp_date_created = null;
            else
                var temp_date_created = "'" + resultObjectSite[i].date_created + "'";

            var queryUpdateSite = "UPDATE site SET " + site_id + " = '" + resultObjectSite[i].site_id.replace("'","''") + "', " + fieldName1 + " = '" + resultObjectSite[i].site_homeowner_Id.replace("'","''") + "', " + fieldName2 + " = " + temp_custrecord_dsg_milestone3_pay_appr_date + ", " + fieldName3 + " = '" + resultObjectSite[i].custrecord_dsg_site_visit_status.replace("'","''") + "', " + fieldName4 + " = '" + resultObjectSite[i].custrecord_dsg_tranche_id.replace("'","''") + "', " + fieldName5 + " = " + temp_custrecord_dsg_site_visit_date + ", " + fieldName6 + " = " + temp_custrecord_dsg_inst2_pay_appr_date + ", " + fieldName7 + " = " + temp_custrecord_dsg_milestone2_pay_appr_date + ", " + fieldName8 + " = " + temp_custrecord_dsg_inst3_pay_appr_date + ", " + fieldName9 + " = '" + resultObjectSite[i].custrecord_dsg_partner.replace("'","''") + "', " + fieldName10 + " = '" + resultObjectSite[i].custrecord_dsg_territory_custom.replace("'","''") + "', " + fieldName11 + " = " + temp_custrecord_dsg_comm_date + ", " + fieldName12 + " = '" + resultObjectSite[i].custrecord_dsg_site_street_1.replace("'","''") + "', " + fieldName13 + " = '" + resultObjectSite[i].custrecord_dsg_site_city.replace("'","''") + "', " + fieldName14 + " = '" + resultObjectSite[i].custrecord_dsg_site_state.replace("'","''") + "', " + fieldName15 + " = '" + resultObjectSite[i].custrecord_dsg_site_zip.replace("'","''") + "', " + fieldName16 + " = '" + resultObjectSite[i].custrecord_dsg_pv_module_model.replace("'","''") + "', " + fieldName17 + " = '" + resultObjectSite[i].custrecord_dsg_pv_modules_qty.replace("'","''") + "', " + fieldName18 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_mfr_1.replace("'","''") + "', " + fieldName19 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_model_1.replace("'","''") + "', " + fieldName20 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_qty.replace("'","''") + "', " + fieldName21 + " = '" + resultObjectSite[i].custrecord_dsg_m3_doc7.replace("'","''") + "', " + fieldName22 + " = '" + resultObjectSite[i].custrecord_dsg_controller_id.replace("'","''") + "', " + fieldName23 + " = '" + resultObjectSite[i].custrecord_dsg_product_type.replace("'","''") + "', " + fieldName24 + " = " + temp_custrecord_dsg_utility_interconnex_date + ", " + fieldName25 + " = " + temp_custrecord_dsg_sys_operation_verify + ", " + fieldName26 + " = '" + resultObjectSite[i].custrecord_dsg_is_homeowner.replace("'","''") + "', " + fieldName27 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_model_2.replace("'","''") + "', " + fieldName28 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_model_3.replace("'","''") + "', " + fieldName29 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_mfr_2.replace("'","''") + "', " + fieldName30 + " = '" + resultObjectSite[i].custrecord_dsg_inverter_mfr_3.replace("'","''") + "', " + fieldName31 + " = '" + resultObjectSite[i].custrecord_dsg_enphase_envoy_id.replace("'","''") + "', " + fieldName32 + " = '" + resultObjectSite[i].custrecord_dsg_pv_module_mfg.replace("'","''") + "', " + fieldName33 + " = '" + resultObjectSite[i].custrecord_dsg_pv_module_size.replace("'","''") + "', " + fieldName34 + " = '" + resultObjectSite[i].custrecord_dsg_monitoring_model.replace("'","''") + "', " + fieldName35 + " = '" + resultObjectSite[i].custrecord_dsg_monitoring_type.replace("'","''") + "', " + fieldName36 + " = '" + resultObjectSite[i].lastModifiedDate + "', " + fieldName37 + " = '" + resultObjectSite[i].lastModifiedDate + "'," + fieldName38 + " = '" + resultObjectSite[i].custrecord_nuvola_tsm.replace("'","''") + "'," + fieldName39 + " = '" + resultObjectSite[i].custrecord_sales_channel.replace("'","''") + "'," + fieldName40 + " = '" + resultObjectSite[i].custrecord_dsg_site_scion_installer.replace("'","''") + "'," + fieldName41 + " = '" + resultObjectSite[i].custrecord_se_nr_so_to_ho_or_id.replace("'","''") + "'," + fieldName42 + " = '" + resultObjectSite[i].custrecord_se_nr_interconnected_so.replace("'","''") + "'," + fieldName43 + " = '" + resultObjectSite[i].custrecord_dsg_tot_contract_amt.replace("'","''") + "'," + fieldName44 + " = '" + resultObjectSite[i].custrecord_se_install_status.replace("'","''") + "'," + fieldName45 + " = '" + resultObjectSite[i].custrecord_se_permitstatus.replace("'","''") + "'," + fieldName46 + " = " + temp_custrecord_dsg_actual_permit_date + "," + fieldName47 + " = '" + resultObjectSite[i].custrecord_dsg_cpm_list.replace("'","''") + "'," + fieldName48 + " = " + temp_custrecord_se_site_visit_sch_date + "," + fieldName49 + " = '" + resultObjectSite[i].custrecord_dsg_site_vst_date_chng_reason.replace("'","''") + "'," + fieldName50 + " = " + temp_custrecord_se_permit_submit_date + "," + fieldName51 + " = " + temp_custrecord_dsg_target_install_comm_date + "," + fieldName52 + " = " + temp_custrecord_dsg_target_permit_date + "," + fieldName53 + " = " + temp_custrecord_dsg_act_install_comm_date + "," + fieldName54 + " = '" + resultObjectSite[i].custrecord_ahj_inspection_status.replace("'","''") + "'," + fieldName55 + " = " + temp_custrecord_dsg_ahj_inspect_approval_date + "," + fieldName56 + " = '" + resultObjectSite[i].custrecord_dsg_inst2_pmt_status.replace("'","''") + "'," + fieldName57 + " = '" + resultObjectSite[i].custrecord_dsg_m2_pmt_status.replace("'","''") + "'," + fieldName58 + " = '" + resultObjectSite[i].custrecord_dsg_m3_stat3.replace("'","''") + "'," + fieldName59 + " = " + temp_custrecord_dsg_m3_dt3 + "," + fieldName60 + " = '" + resultObjectSite[i].custrecord_dsg_m3_stat4.replace("'","''") + "'," + fieldName61 + " = '" + resultObjectSite[i].custrecord_dsg_inst3_pmt_status.replace("'","''") + "'," + fieldName62 + " = '" + resultObjectSite[i].custrecord_dsg_m3_pmt_status.replace("'","''") + "'," + fieldName63 + " = '" + resultObjectSite[i].custrecord_dsg_lien_doc_status.replace("'","''") + "'," + fieldName64 + " = " + temp_custrecord_dsg_se2_pay_appr_date + "," + fieldName65 + " = '" + resultObjectSite[i].site_homeowner_Name.replace("'","''") + "'," + fieldName66 + " = " + temp_custrecord_dsg_target_install_date + "," + fieldName67 + " = " + temp_date_created + " WHERE record = '" + recordId + "'";
            
            client.query(queryUpdateSite, function(err,result){
                if(err){
                    return console.log('Error returning query (Update Site)' + err + queryUpdateSite);
                }
                console.log('Site Updated');
                //console.log(queryUpdateSite);
                return client;
            });
        }
    });
}

var insertNetsuitePromiseData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchPromise, function(error,result){
            if(error){
                console.log("Error returning query", error);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            console.log("Last known timestamp " + lastModifiedDate);

            var postDataPromise = {recordtype:'promise_data',timestamp:lastModifiedDate};
            var postStringPromise = JSON.stringify(postDataPromise);
            var reqPromise = http.request(options, function(response) {

                console.log("This goes to netsuite" + postStringPromise);
                response.setEncoding('utf-8');
                
                console.log("Received response(site): " + response.statusCode);
                response.on("data",function(dataPromise){
                    responseStringPromise += dataPromise;
                });

                response.on('end', function() {
                    var resultObjectPromise = JSON.parse(responseStringPromise);
                    for(var i=0;i<resultObjectPromise.length;i++){
                        var recordId = resultObjectPromise[i].recordId;
                        if(resultObjectPromise[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBPromiseData(i, resultObjectPromise);
                        }
                        
                    }
                });
                
            });
            reqPromise.on('error', function(e) {
                console.log("Error" + e);
            });
                
            reqPromise.write(postStringPromise);
            reqPromise.end();

        });

        var fetchDuplicateRowsFromDBPromiseData = function(i, resultObjectPromise){
            var recordId = resultObjectPromise[i].recordId;
            var checkDuplicateRecordQuery = "SELECT * from promise_data where record = '" + recordId + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuitePromiseData(i,resultObjectPromise);
                }
                else{
                    updateIntoRedshiftNetsuitePromiseData(i, recordId, "promise_type","promise_jan","promise_feb","promise_mar","promise_apr","promise_may","promise_june","promise_july","promise_aug","promise_sept","promise_oct","promise_nov","promise_dec","degradation_rate","expected_to_promise_ratio","last_timestamp","last_modified_date","homeowner_id","site_id", resultObjectPromise);
                }
            });
        }
        
        var insertIntoRedshiftNetsuitePromiseData = function(i,resultObjectPromise){
            client.query(queryTestInsertPromise, [resultObjectPromise[i].recordId, resultObjectPromise[i].custrecord473, resultObjectPromise[i].custrecord445_2, resultObjectPromise[i].custrecord446_2, resultObjectPromise[i].custrecord447_2, resultObjectPromise[i].custrecord448_2, resultObjectPromise[i].custrecord449_2, resultObjectPromise[i].custrecord450_2, resultObjectPromise[i].custrecord451_2, resultObjectPromise[i].custrecord452, resultObjectPromise[i].custrecord453, resultObjectPromise[i].custrecord454, resultObjectPromise[i].custrecord455, resultObjectPromise[i].custrecord456, resultObjectPromise[i].custrecord470, resultObjectPromise[i].custrecord472, resultObjectPromise[i].lastModifiedDate, resultObjectPromise[i].lastModifiedDate, resultObjectPromise[i].homeowner_id, resultObjectPromise[i].site_id], function(err,result){
                if(err){
                    return console.log('Error returning query (Insert Promise)', err);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(Promise): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuitePromiseData = function(i, recordId, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6,fieldName7,fieldName8,fieldName9,fieldName10,fieldName11,fieldName12,fieldName13,fieldName14,fieldName15,fieldName16,fieldName17,fieldName18, fieldName19, resultObjectPromise){
            var queryUpdatePromise = "UPDATE promise_data SET " + fieldName1 + " = '" + resultObjectPromise[i].custrecord473 + "', " + fieldName2 + " = '" + resultObjectPromise[i].custrecord445_2 + "', " + fieldName3 + " = '" + resultObjectPromise[i].custrecord446_2 + "', " + fieldName4 + " = '" + resultObjectPromise[i].custrecord447_2 + "', " + fieldName5 + " = '" + resultObjectPromise[i].custrecord448_2 + "', " + fieldName6 + " = '" + resultObjectPromise[i].custrecord449_2 + "', " + fieldName7 + " = '" + resultObjectPromise[i].custrecord450_2 + "', " + fieldName8 + " = '" + resultObjectPromise[i].custrecord451_2 + "', " + fieldName9 + " = '" + resultObjectPromise[i].custrecord452 + "', " + fieldName10 + " = '" + resultObjectPromise[i].custrecord453 + "', " + fieldName11 + " = '" + resultObjectPromise[i].custrecord454 + "', " + fieldName12 + " = '" + resultObjectPromise[i].custrecord455 + "', " + fieldName13 + " = '" + resultObjectPromise[i].custrecord456 + "', " + fieldName14 + " = '" + resultObjectPromise[i].custrecord470 + "', " + fieldName15 + " = '" + resultObjectPromise[i].custrecord472 + "', "+ fieldName16 + " = '" + resultObjectPromise[i].lastModifiedDate + "', " + fieldName17 + " = '" + resultObjectPromise[i].lastModifiedDate + fieldName18 + " = '" + resultObjectPromise[i].homeowner_id + fieldName19 + " = '" + resultObjectPromise[i].site_id + "' WHERE record = '" + recordId + "'";
            //console.log(queryUpdatePromise);
            client.query(queryUpdatePromise, function(err,result){
                if(err){
                    return console.log('Error returning query (Update Promise)', err);
                }
                console.log('Updated Promise');
                return client;
            });
        }
    });
}

var insertNetsuiteCaseData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchCase, function(error,result){
            if(error){
                console.log("Error returning query", error);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            console.log("Last known timestamp " + lastModifiedDate);

            var postDataCase = {recordtype:'case',timestamp:lastModifiedDate};
            var postStringCase = JSON.stringify(postDataCase);
            var reqCase = http.request(options, function(response) {

                console.log("This goes to netsuite" + postStringCase);
                response.setEncoding('utf-8');
                
                console.log("Received response(site): " + response.statusCode);
                response.on("data",function(dataCase){
                    responseStringCase += dataCase;
                });

                response.on('end', function() {
                    var resultObjectCase = JSON.parse(responseStringCase);
                    for(var i=0;i<resultObjectCase.length;i++){
                        var recordId = resultObjectCase[i].recordId;
                        if(resultObjectCase[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBCaseData(i, resultObjectCase);
                        }
                        
                    }
                });
                
            });
            reqCase.on('error', function(e) {
                console.log("Error" + e);
            });
                
            reqCase.write(postStringCase);
            reqCase.end();

        });
        
        var fetchDuplicateRowsFromDBCaseData = function(i, resultObjectCase){
            var recordId = resultObjectCase[i].recordId;
            var checkDuplicateRecordQuery = "SELECT * from case_data where record = '" + recordId + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuiteCaseData(i,resultObjectCase);
                }
                else{
                    updateIntoRedshiftNetsuiteCaseData(i, recordId, "case_number","title","status","startdate","last_timestamp","last_modified_date", "homeowner_id","site_id", resultObjectCase);
                }
            });
        }

        var insertIntoRedshiftNetsuiteCaseData = function(i,resultObjectCase){
            if(resultObjectCase[i].startdate == '')
                var temp_startdate = null;
            else
                var temp_startdate = "'" + resultObjectCase[i].startdate + "'";

            var queryTestInsertCase = "INSERT INTO CASE_DATA (record, case_number, title, status, startdate, last_timestamp, last_modified_date, homeowner_id, site_id) values ('" + resultObjectCase[i].recordId.replace("'","''") + "','" + resultObjectCase[i].casenumber.replace("'","''") + "','" + resultObjectCase[i].title.replace("'","''") + "','" + resultObjectCase[i].status.replace("'","''") + "'," + temp_startdate + ",'" + resultObjectCase[i].lastModifiedDate + "','" + resultObjectCase[i].lastModifiedDate + "','" + resultObjectCase[i].homeowner_id.replace("'","''") + "','" + resultObjectCase[i].site_id.replace("'","''") + "')"; 
            client.query(queryTestInsertCase, function(err,result){
                //console.log(queryTestInsertCase);
                if(err){
                    return console.log('Error returning query (Insert Case)' + err + queryTestInsertCase);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(Case): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuiteCaseData = function(i, recordId, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6, fieldName7, fieldName8, resultObjectCase){
            if(resultObjectCase[i].startdate == '')
                var temp_startdate = null;
            else
                var temp_startdate = "'" + resultObjectCase[i].startdate + "'";
            var queryUpdateCase = "UPDATE case_data SET " + fieldName1 + " = '" + resultObjectCase[i].casenumber.replace("'","''") + "', " + fieldName2 + " = '" + resultObjectCase[i].title.replace("'","''") + "', " + fieldName3 + " = '" + resultObjectCase[i].status.replace("'","''") + "', " + fieldName4 + " = " + temp_startdate + ", " + fieldName5 + " = '" + resultObjectCase[i].lastModifiedDate + "', " + fieldName6 + " = '" + resultObjectCase[i].lastModifiedDate + "', " + fieldName7  + " = '" + resultObjectCase[i].homeowner_id.replace("'","''") + "', " + fieldName8  + " = '" + resultObjectCase[i].site_id.replace("'","''") + "' WHERE record = '" + recordId + "'";
            //console.log(queryUpdateCase);
            client.query(queryUpdateCase, function(err,result){
                if(err){
                    return console.log('Error returning query (Update Case)' + err + queryUpdateCase);
                }
                console.log('Updated Case Data.');
                return client;
            });
        }
    });
}

var insertNetsuiteArrayInfoData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchArrayInfo, function(error,result){
            if(error){
                console.log("Error returning query", error);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            console.log("Last known timestamp " + lastModifiedDate);

            var postDataArrayInfo = {recordtype:'array_info',timestamp:lastModifiedDate};
            var postStringArrayInfo = JSON.stringify(postDataArrayInfo);
            var reqCase = http.request(options, function(response) {

                console.log("This goes to netsuite" + postStringArrayInfo);
                response.setEncoding('utf-8');
                
                console.log("Received response(site): " + response.statusCode);
                response.on("data",function(dataArrayInfo){
                    responseStringArrayInfo += dataArrayInfo;
                });

                response.on('end', function() {
                    var resultObjectArrayInfo = JSON.parse(responseStringArrayInfo);
                    for(var i=0;i<resultObjectArrayInfo.length;i++){
                        var recordId = resultObjectArrayInfo[i].recordId;
                        if(resultObjectArrayInfo[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBArrayInfo(i, resultObjectArrayInfo);
                        }
                        
                    }
                });
                
            });
            reqCase.on('error', function(e) {
                console.log("Error" + e);
            });
                
            reqCase.write(postStringArrayInfo);
            reqCase.end();

        });

        var fetchDuplicateRowsFromDBArrayInfo = function(i, resultObjectArrayInfo){
            var recordId = resultObjectArrayInfo[i].recordId;
            var checkDuplicateRecordQuery = "SELECT * from array_info where record = '" + recordId + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuiteArrayInfoData(i,resultObjectArrayInfo);
                }
                else{
                    updateIntoRedshiftNetsuiteArrayInfoData(i, recordId, "array_number","thm_module_qty_roof","roof_tilt_in_degrees","orientation","solar_access_jan","solar_access_feb","solar_access_mar","solar_access_apr","solar_access_may","solar_access_june","solar_access_july","solar_access_aug","solar_access_sept","solar_access_oct","solar_access_nov","solar_access_dec","last_timestamp","last_modified_date","homeowner_id","site_id", resultObjectArrayInfo);
                }
            });
        }

        var insertIntoRedshiftNetsuiteArrayInfoData = function(i,resultObjectArrayInfo){
            client.query(queryTestInsertArrayInfo, [resultObjectArrayInfo[i].recordId, resultObjectArrayInfo[i].custrecord206, resultObjectArrayInfo[i].custrecord208, resultObjectArrayInfo[i].custrecord209, resultObjectArrayInfo[i].custrecord210, resultObjectArrayInfo[i].custrecord212, resultObjectArrayInfo[i].custrecord213, resultObjectArrayInfo[i].custrecord214, resultObjectArrayInfo[i].custrecord215, resultObjectArrayInfo[i].custrecord216, resultObjectArrayInfo[i].custrecord217, resultObjectArrayInfo[i].custrecord218, resultObjectArrayInfo[i].custrecord219, resultObjectArrayInfo[i].custrecord220, resultObjectArrayInfo[i].custrecord221, resultObjectArrayInfo[i].custrecord222, resultObjectArrayInfo[i].custrecord223, resultObjectArrayInfo[i].lastModifiedDate, resultObjectArrayInfo[i].lastModifiedDate,resultObjectArrayInfo[i].homeowner_id,resultObjectArrayInfo[i].site_id], function(err,result){
                if(err){
                    return console.log('Error returning query', err);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(Array_info): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuiteArrayInfoData = function(i, recordId, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6,fieldName7,fieldName8,fieldName9,fieldName10,fieldName11,fieldName12,fieldName13,fieldName14,fieldName15,fieldName16,fieldName17,fieldName18,fieldName19, fieldName20, resultObjectArrayInfo){
            var queryUpdateArrayInfo = "UPDATE array_info SET " + fieldName1 + " = '" + resultObjectArrayInfo[i].custrecord206 + "', " + fieldName2 + " = '" + resultObjectArrayInfo[i].custrecord208 + "', " + fieldName3 + " = '" + resultObjectArrayInfo[i].custrecord209 + "', " + fieldName4 + " = '" + resultObjectArrayInfo[i].custrecord210 + "', " + fieldName5 + " = '" + resultObjectArrayInfo[i].custrecord212 + "', " + fieldName6 + " = '" + resultObjectArrayInfo[i].custrecord213 + "', " + fieldName7 + " = '" + resultObjectArrayInfo[i].custrecord214 + "', " + fieldName8 + " = '" + resultObjectArrayInfo[i].custrecord215 + "', " + fieldName9 + " = '" + resultObjectArrayInfo[i].custrecord216 + "', " + fieldName10 + " = '" + resultObjectArrayInfo[i].custrecord217 + "', " + fieldName11 + " = '" + resultObjectArrayInfo[i].custrecord218 + "', " + fieldName12 + " = '" + resultObjectArrayInfo[i].custrecord219 + "', " + fieldName13 + " = '" + resultObjectArrayInfo[i].custrecord220 + "', " + fieldName14 + " = '" + resultObjectArrayInfo[i].custrecord221 + "', " + fieldName15 + " = '" + resultObjectArrayInfo[i].custrecord222 + "', "+ fieldName16 + " = '" + resultObjectArrayInfo[i].custrecord223  + "', "+ fieldName17 + " = '" + resultObjectArrayInfo[i].lastModifiedDate + "', " + fieldName18 + " = '" + resultObjectArrayInfo[i].lastModifiedDate + "', "+ fieldName19 + " = '" + resultObjectArrayInfo[i].homeowner_id + "', "+ fieldName20 + " = '" + resultObjectArrayInfo[i].site_id + "' WHERE record = '" + recordId + "'";
            //console.log(queryUpdateArrayInfo);
            client.query(queryUpdateArrayInfo, function(err,result){
                if(err){
                    return console.log('Error returning query', err);
                }
                console.log('Updated Array Info');
                return client;
            });
        }
    });
}

var insertNetsuiteSalesOrderData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        var queryFetchSalesOrder = 'SELECT * from SALES_ORDER ORDER BY LAST_TIMESTAMP DESC LIMIT 1';
        client.query(queryFetchSalesOrder, function(error,result){
            if(error){
                console.log("Error returning query", error);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            var uniqueRecordId = result.rows[0].record;
            console.log("Last known timestamp " + lastModifiedDate);

            var postDataSalesOrder = {recordtype:'sales_order',recordId:uniqueRecordId,timestamp:lastModifiedDate};
            var postStringSalesOrder = JSON.stringify(postDataSalesOrder);
            var reqCase = http.request(options, function(response) {

                console.log("This goes to netsuite" + postStringSalesOrder);
                response.setEncoding('utf-8');
                
                console.log("Received response(site): " + response.statusCode);
                response.on("data",function(dataSalesOrder){
                    responseStringSalesOrder += dataSalesOrder;
                });

                response.on('end', function() {
                    var resultObjectSalesOrder = JSON.parse(responseStringSalesOrder);
                    for(var i=0;i<resultObjectSalesOrder.length;i++){
                        var recordId = resultObjectSalesOrder[i].recordId;
                        if(resultObjectSalesOrder[0].errormsg == 'No More Results'){
                            console.log("Nothing to load.");
                        }
                        else{
                            fetchDuplicateRowsFromDBSalesOrder(i, resultObjectSalesOrder);
                        }
                        
                    }
                });
                
            });
            reqCase.on('error', function(e) {
                console.log("Error" + e);
            });
                
            reqCase.write(postStringSalesOrder);
            reqCase.end();

        });

        var fetchDuplicateRowsFromDBSalesOrder = function(i, resultObjectSalesOrder){
            var recordId = resultObjectSalesOrder[i].recordId;
            var number = resultObjectSalesOrder[i].number;
            var checkDuplicateRecordQuery = "SELECT * from sales_order where number = '" + number + "'";
            client.query(checkDuplicateRecordQuery,function(err,resultLast){
                if(resultLast.rows.length == 0){
                    insertIntoRedshiftNetsuiteSalesOrderData(i,resultObjectSalesOrder);
                }
                else{
                    updateIntoRedshiftNetsuiteSalesOrderData(i, number, "actual_delivery_date","number","datecreated","pv_total_kw_ordered","last_timestamp","last_modified_date","homeowner_id","site_id","orderstatus","current_promise_date", resultObjectSalesOrder);
                }
            });
        }

        var insertIntoRedshiftNetsuiteSalesOrderData = function(i,resultObjectSalesOrder){
            if(resultObjectSalesOrder[i].custbody20 == '')
                var temp_actual_delivery_date = null;
            else
                var temp_actual_delivery_date = "'" + resultObjectSalesOrder[i].custbody20 + "'";

            if(resultObjectSalesOrder[i].datecreated == '')
                var temp_datecreated = null;
            else
                var temp_datecreated = "'" + resultObjectSalesOrder[i].datecreated + "'";

            if(resultObjectSalesOrder[i].custbody_currentpromiseddate == '')
                var temp_current_promise_date = null;
            else
                var temp_current_promise_date = "'" + resultObjectSalesOrder[i].custbody_currentpromiseddate + "'";

            var queryTestInsertSalesOrder = "INSERT INTO sales_order (record, actual_delivery_date, number, datecreated, pv_total_kw_ordered, last_timestamp, last_modified_date, homeowner_id, site_id, orderstatus, current_promise_date) values ('" + resultObjectSalesOrder[i].recordId.replace("'","''") + "'," + temp_actual_delivery_date + ",'" + resultObjectSalesOrder[i].number.replace("'","''") + "'," + temp_datecreated + ",'" + resultObjectSalesOrder[i].custbody_pvtotalkwordered.replace("'","''") + "','" + resultObjectSalesOrder[i].lastModifiedDate + "','" + resultObjectSalesOrder[i].lastModifiedDate + "','" + resultObjectSalesOrder[i].homeowner_id.replace("'","''") + "','" + resultObjectSalesOrder[i].site_id.replace("'","''") + "','" + resultObjectSalesOrder[i].orderstatus.replace("'","''") + "'," + temp_current_promise_date +")";
            client.query(queryTestInsertSalesOrder, function(err,result){
                if(err){
                    return console.log('Error returning query' + err + queryTestInsertSalesOrder);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(SalesOrder): ' + result);
                return client;
            });
        }

        var updateIntoRedshiftNetsuiteSalesOrderData = function(i, number, fieldName1,fieldName2,fieldName3,fieldName4,fieldName5,fieldName6,fieldName7,fieldName8,fieldName9,fieldName10, resultObjectSalesOrder){
            if(resultObjectSalesOrder[i].custbody20 == '')
                var temp_actual_delivery_date = null;
            else
                var temp_actual_delivery_date = "'" + resultObjectSalesOrder[i].custbody20 + "'";

            if(resultObjectSalesOrder[i].datecreated == '')
                var temp_datecreated = null;
            else
                var temp_datecreated = "'" + resultObjectSalesOrder[i].datecreated + "'";

            if(resultObjectSalesOrder[i].custbody_currentpromiseddate == '')
                var temp_current_promise_date = null;
            else
                var temp_current_promise_date = "'" + resultObjectSalesOrder[i].custbody_currentpromiseddate + "'";

            var queryUpdateSalesOrder = "UPDATE sales_order SET " + fieldName1 + " = " + temp_actual_delivery_date + ", " + fieldName2 + " = '" + resultObjectSalesOrder[i].number.replace("'","''") + "', " + fieldName3 + " = " + temp_datecreated + ", " + fieldName4 + " = '" + resultObjectSalesOrder[i].custbody_pvtotalkwordered.replace("'","''") + "', " + fieldName5 + " = '" + resultObjectSalesOrder[i].lastModifiedDate + "', " + fieldName6 + " = '" + resultObjectSalesOrder[i].lastModifiedDate + "', " + fieldName7 + " = '" + resultObjectSalesOrder[i].homeowner_id.replace("'","''") + "', " + fieldName8 + " = '" + resultObjectSalesOrder[i].site_id.replace("'","''") + "', " + fieldName9 + " = '" + resultObjectSalesOrder[i].orderstatus.replace("'","''") + "', " + fieldName10 + " = " + temp_current_promise_date + " WHERE number = '" + number + "'";
            client.query(queryUpdateSalesOrder, function(err,result){
                if(err){
                    return console.log('Error returning query' + err + queryUpdateSalesOrder);
                }
                console.log('Updated Sales Order');
                return client;
            });
        }
    });
}


/* var insertNetsuiteContactData = function(){
    pg.connect(conString, function(err,client){
        if(err){
            context.done("Fatal Error");
        }
        client.query(queryFetchContactTest, function(err,result){
            if(err){
                console.log("Error returning query", err);
                context.done("Fatal Error");
            }

            else if(result.rows.length == 0){
                context.done("No record found");
            }
            var lastModifiedDate = result.rows[0].last_modified_date;
            console.log("Last known timestamp " + lastModifiedDate);

            var postData = {recordtype:'contact',timestamp:lastModifiedDate};
            var postString = JSON.stringify(postData);
            var req = http.request(options, function(res) {

                console.log("This goes to netsuite for contact" + postString);
                res.setEncoding('utf-8');
                
                console.log("Received response for contact: " + res.statusCode);
                res.on("data",function(data){
                    responseString += data;
                });

                res.on('end', function() {
                    var resultObject = JSON.parse(responseString);
                    for(var i=0;i<resultObject.length;i++){
                        insertIntoRedshiftNetsuiteContactData(i,resultObject);
                    }
                });
                
            });
            req.on('error', function(e) {
                console.log("Error" + e);
            });
                
            req.write(postString);
            req.end();

        });
        var insertIntoRedshiftNetsuiteContactData = function(i,resultObject){
            client.query(queryTestContactInsertTest, [resultObject[i].record,resultObject[i].firstname,resultObject[i].lastname,resultObject[i].email,resultObject[i].lastModifiedDate,resultObject[i].lastModifiedDate], function(err,result){
                if(err){
                    return console.log('Error returning query', err);
                }
                console.log('Row inserted from Request JSON. Go and check on Redshift(For contact): ' + result);
                return client;
            });
        }
    });
} */


