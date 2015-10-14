/**
 * Copyright (c) 2015 Sun Edison, Inc.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * SunEdison, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Sun Edison.

 */

/**
 * Module Description
 * 
 * Version    Date            Author           Remarks
 * 1.00      22Jul2015     Sandeep Saxena      Intial Version
 * 
 *
 */

//Do not add the columns which creates duplicate Results (like ZipCode)..
function checkTimeStamp(datain) 
{
    var err = new Object();

    if (!datain.recordtype) {
        err.status = "failed";
        err.message = "missing recordtype";
        return err;
    }

    var response;
    var timestamp = datain.timestamp;
    var recordtype = datain.recordtype//Sample TimeStamp format "6/27/2015 11:11 pm"
    var lastRecordId = datain.recordId ; 
    nlapiLogExecution('DEBUG','checkTimeStamp','timestamp='+ timestamp +", recordtype="+ recordtype+", lastRecordId="+ lastRecordId);
	   

    if(recordtype==="customer") 
    {
    	response = getCustomerData(timestamp,lastRecordId); 
    }   
    else if(recordtype=="site")
    {
    	response = getSiteData(timestamp,lastRecordId);
    }
    else if(recordtype=="promise_data")
    {
    	response = getPromiseData(timestamp,lastRecordId);
    }
    else if(recordtype=="array_info")
    {
    	response = getArrayInfoData(timestamp,lastRecordId);
    }
    else if(recordtype=="case")
    {
    	response = getCaseData(timestamp,lastRecordId);
    }
    else if(recordtype=="sales_order")
    {
    	response = getSalesOrderData(timestamp,lastRecordId);
    }

    nlapiLogExecution('DEBUG','checkTimeStamp','output='+ JSON.stringify(response));
    return response ;
}

function parseDateString(ds){
	var a = ds.split(' '); // break date from time
	var d = a[0].split('/'); // break date into year, month day
	var t = a[1].split(':'); // break time into hours, minutes, seconds
	nlapiLogExecution('DEBUG','parseDateString'," date = "+d[0]+"/"+d[1]+"/"+d[2]+" "+t[0]+":"+t[1]);
	var date = d[0]+"/"+d[1]+"/"+d[2]+" "+t[0]+":"+t[1];
	nlapiLogExecution('DEBUG','parseDateString','date1 =' +date);

	return date//new Date(d[0], d[1]-1, d[2], t[0], t[1]);//, t[2]);
}
// Get Customer Data by creating search
// Do not add the columns which creates duplicate Results..
function getCustomerData(timestamp,lastRecordId){
	
	//var search_length;	
	var results = [];
		var filters = new Array();
		var columns = new Array();
	    filters[0] = new nlobjSearchFilter('lastmodifieddate', null, 'onOrAfter', timestamp);
		columns[0] = new nlobjSearchColumn('firstname');
		columns[1] = new nlobjSearchColumn('lastname');
		columns[2] = new nlobjSearchColumn('email');
		columns[3] = new nlobjSearchColumn('lastmodifieddate' );
	//	columns[4] = new nlobjSearchColumn('zipcode');//
		columns[4] = new nlobjSearchColumn('custentity_contract_canceled_date');
		columns[5] = new nlobjSearchColumn('custentity200');
		columns[6] = new nlobjSearchColumn('custentity206');
		columns[7] = new nlobjSearchColumn('custentity205');
		columns[8] = new nlobjSearchColumn('custentity_proposalstatus');
		columns[9] = new nlobjSearchColumn('custentity_dsg_partner_sub_type');
	//	columns[11] = new nlobjSearchColumn('custentity_sitevisitstatus');
		columns[10]= new nlobjSearchColumn('custentity21');
	//	columns[13] = new nlobjSearchColumn('custentity_se_sales_channel');
		columns[11] = new nlobjSearchColumn('custentity_nuv_fully_exec_date');
		columns[12] = new nlobjSearchColumn('custentity_assign_partner')
		columns[13] = new nlobjSearchColumn('custentity239')
		columns[14] = new nlobjSearchColumn('custentity208')
		columns[15] = new nlobjSearchColumn('custentity209')
		//columns[19] = new nlobjSearchColumn('custentity210')
		columns[16] = new nlobjSearchColumn('custentity_executed_assign_agrmnt_date')
		columns[17] = new nlobjSearchColumn('custentity_dsg_lead_site_id') 
		columns[18] = new nlobjSearchColumn('custentity387');
		columns[19] = new nlobjSearchColumn('datecreated')
		columns[20] = new nlobjSearchColumn('custentity_proposaldate')
		columns[21] = new nlobjSearchColumn('custentity204')
		columns[22] = new nlobjSearchColumn('custentity_executed_assign_agmnt_status')
		//columns[19] = new nlobjSearchColumn('custentity210')
		columns[23] = new nlobjSearchColumn('subsidiarynohierarchy')
		columns[24] = new nlobjSearchColumn('custentity_nuvola_pda_signeddate') 
		columns[25] = new nlobjSearchColumn('internalid')
		columns[3].setSort();
		columns[25].setSort();
	
		var searchresults = nlapiSearchRecord('customer', null, filters, columns );
	    if(searchresults) 
	    {
	    	nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
			if(lastRecordId)
			{
				index = getRecordIdIndex(searchresults,lastRecordId,timestamp);				
			}
			else
			{
				index = 1;
			}
			var search_length = searchresults.length;
			if(search_length>100)
			{
				search_length = 100;
			}
			
			for ( var i = index; searchresults != null && i <= search_length; i++ )
			{
				try
				{
					var result={};
				
					var searchresult  = searchresults[ i ];
					result.recordId = searchresult.getId( );
					result.lastModifiedDate = searchresult.getValue('lastmodifieddate');
					result.firstname = searchresult.getValue('firstname');		
					result.email = searchresult.getValue('email');
					result.lastname = searchresult.getValue( 'lastname' );
				//	result.zipcode = searchresult.getValue( 'zipcode');
					result.custentity_contract_canceled_date = searchresult.getValue('custentity_contract_canceled_date');
					result.custentity200 = searchresult.getText('custentity200'); // get Purchase Type.
					result.Fully_Executed_Date = searchresult.getValue('custentity_nuv_fully_exec_date');
					result.custentity206 = searchresult.getText('custentity206');  // get Homeowner Proj Document Status.
					result.custentity205 = searchresult.getText('custentity205');  // get Homeowner Lease Contract Status
					result.custentity_proposalstatus = searchresult.getText('custentity_proposalstatus');
					result.custentity_dsg_partner_sub_type = searchresult.getText( 'custentity_dsg_partner_sub_type' );
			//		result.custentity_sitevisitstatus = searchresult.getText( 'custentity_sitevisitstatus');
					result.custentity21 = searchresult.getValue('custentity21'); // get Proj Definition Doc Last Update Date
			//		result.custentity_se_sales_channel = searchresult.getText('custentity_se_sales_channel');
					result.custentity_assign_partner = searchresult.getText('custentity_assign_partner');
					result.custentity239 = searchresult.getValue('custentity239'); // get Proposed System Size (PV/kWh)
					result.custentity208 = searchresult.getValue( 'custentity208' ); // get Credit Check Last Update Date
					result.custentity209 = searchresult.getValue( 'custentity209'); // get Gas Utility Info
					//result.custentity210 = searchresult.getValue('custentity210'); // get Proj Definition Doc Last Update Date
					result.custentity_executed_assign_agrmnt_date = searchresult.getValue('custentity_executed_assign_agrmnt_date');
					result.site_id = searchresult.getText('custentity_dsg_lead_site_id');
					result.custentity387  = searchresult.getText('custentity387'); // financing Program
					result.datecreated = searchresult.getValue('datecreated');
					result.custentity_proposaldate = searchresult.getValue('custentity_proposaldate'); 
					result.custentity204 = searchresult.getText( 'custentity204'); // HomeOwner Credit Check Status 
					result.custentity_executed_assign_agmnt_status = searchresult.getText( 'custentity_executed_assign_agmnt_status'); // get Gas Utility Info
					result.subsidiarynohierarchy = searchresult.getText('subsidiarynohierarchy'); 
					result.custentity_nuvola_pda_signeddate = searchresult.getValue('custentity_nuvola_pda_signeddate');
				//	result.site_id = searchresult.getValue('custentity_dsg_lead_site_id');
					results.push(result);
				}
				catch (e)
				{
					nlapiLogExecution('EMERGENCY','Error in getCustomerData ',e);
					continue;
				}
			}
	   }
	    else 
	    {
	    	var error={}; 
	    	error.errormsg = 'No More Results';    
	    	results.push(error);
	    }
    return results;
	
}

// Get *DSG Site Record (PROD) Data
function getSiteData(timestamp,lastRecordId){
   
	var index;	
	var results = [];
	var filters = new Array();
	var columns = new Array();
	filters[0] = new nlobjSearchFilter('lastmodified', 'null', 'onOrAfter', timestamp);

		columns[0] = new nlobjSearchColumn('custrecord_dsg_site_homeowner');
		columns[1] = new nlobjSearchColumn('custrecord_dsg_milestone3_pay_appr_date');
		columns[2] = new nlobjSearchColumn('custrecord_dsg_site_visit_status');
		columns[3] = new nlobjSearchColumn('lastmodified' );
		columns[4] = new nlobjSearchColumn('custrecord_dsg_tranche_id');//
		columns[5] = new nlobjSearchColumn('custrecord_dsg_site_visit_date');
		columns[6] = new nlobjSearchColumn('custrecord_dsg_inst2_pay_appr_date');
		columns[7] = new nlobjSearchColumn('custrecord_dsg_milestone2_pay_appr_date');
		columns[8] = new nlobjSearchColumn('custrecord_dsg_inst3_pay_appr_date');
		columns[9] = new nlobjSearchColumn('custrecord_dsg_partner');
		columns[10] = new nlobjSearchColumn('custrecord_dsg_site_homeowner');
		columns[11] = new nlobjSearchColumn('custrecord_dsg_territory_custom');
		columns[12]= new nlobjSearchColumn('custrecord_dsg_site_visit_status');
		columns[13] = new nlobjSearchColumn('custrecord_dsg_comm_date');
		columns[14] = new nlobjSearchColumn('custrecord_dsg_site_street_1');
		columns[15] = new nlobjSearchColumn('custrecord_dsg_site_city')
		columns[16] = new nlobjSearchColumn('custrecord_dsg_site_state')
		columns[17] = new nlobjSearchColumn('custrecord_dsg_site_zip')
		columns[18] = new nlobjSearchColumn('custrecord_dsg_pv_module_model')
		columns[19] = new nlobjSearchColumn('custrecord_dsg_pv_modules_qty')
		columns[20] = new nlobjSearchColumn('custrecord_dsg_inverter_mfr_1')
		columns[21] = new nlobjSearchColumn('custrecord_dsg_inverter_model_1')
		columns[22] = new nlobjSearchColumn('custrecord_dsg_inverter_qty')
		columns[23] = new nlobjSearchColumn('custrecord_dsg_m3_doc7')
		columns[24] = new nlobjSearchColumn('custrecord_dsg_controller_id');
		columns[25] = new nlobjSearchColumn('custrecord_dsg_product_type');
		columns[26] = new nlobjSearchColumn('custrecord_dsg_site_financing_program');
		columns[27] = new nlobjSearchColumn('custrecord_dsg_comm_date' );
		columns[28] = new nlobjSearchColumn('custrecord_dsg_utility_interconnex_date');//
		columns[29] = new nlobjSearchColumn('custrecord_dsg_sys_operation_verify');
		columns[30] = new nlobjSearchColumn('custrecord_dsg_is_homeowner');
		columns[31] = new nlobjSearchColumn('custrecord_dsg_inverter_model_1');
		columns[32] = new nlobjSearchColumn('custrecord_dsg_inverter_model_2');
		columns[33] = new nlobjSearchColumn('custrecord_dsg_inverter_model_3');
		columns[34] = new nlobjSearchColumn('custrecord_dsg_inverter_mfr_1');
		columns[35] = new nlobjSearchColumn('custrecord_dsg_inverter_mfr_2');
		columns[36]= new nlobjSearchColumn('custrecord_dsg_inverter_mfr_3');
		columns[37] = new nlobjSearchColumn('custrecord_dsg_enphase_envoy_id');
		columns[38] = new nlobjSearchColumn('custrecord_dsg_pv_module_mfg');
		columns[39] = new nlobjSearchColumn('custrecord_dsg_pv_module_model')
		columns[40] = new nlobjSearchColumn('custrecord_dsg_pv_module_size')
		columns[41] = new nlobjSearchColumn('custrecord_dsg_monitoring_model')
		columns[42] = new nlobjSearchColumn('custrecord_dsg_monitoring_type')
		columns[43] = new nlobjSearchColumn('custrecord_nuvola_tsm')
		columns[44] = new nlobjSearchColumn('custrecord_sales_channel')
		columns[45] = new nlobjSearchColumn('custrecord_dsg_site_scion_installer')
		columns[46] = new nlobjSearchColumn('name'); 
		columns[47] = new nlobjSearchColumn('custrecord_dsg_tot_contract_amt');
		columns[48] = new nlobjSearchColumn('internalid')
        columns[49] = new nlobjSearchColumn('custrecord_se_install_status');        	
		columns[50] = new nlobjSearchColumn('formulatext').setFormula('{custrecord_se_nr_so_to_ho_or_id}');
		columns[51] = new nlobjSearchColumn('formulatext').setFormula('{custrecord_se_nr_interconnected_so}');
		columns[52] = new nlobjSearchColumn('custrecord_se_permitstatus')
        columns[53] = new nlobjSearchColumn('custrecord_dsg_actual_permit_date');
		columns[48].setSort();// Sorting by Internal ID
		columns[3].setSort();	//Sorting by Last modified Time     

		var searchresults = nlapiSearchRecord('customrecord_dsg_site_qa', null, filters, columns );
		if(searchresults)
		{
			nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
			if(lastRecordId)
			{
				index = getRecordIdIndex(searchresults,lastRecordId,timestamp);
				
			}
			else
			{
				index = 1;
			}
			var search_length = searchresults.length;
			if(search_length>100)
			{
				search_length = 100;
			}
			for ( var i = index; searchresults != null && i <= search_length ; i++ )
			{			
				try 
				{ 
					var result={};
			        
					var searchresult  = searchresults[ i ];
					result.recordId = searchresult.getId( );
					result.site_id = searchresult.getValue('name' );
					result.lastModifiedDate = searchresult.getValue('lastmodified');
					result.custrecord_dsg_site_homeowner = searchresult.getValue('custrecord_dsg_site_homeowner');		
					result.custrecord_dsg_milestone3_pay_appr_date = searchresult.getValue('custrecord_dsg_milestone3_pay_appr_date');
					result.custrecord_dsg_site_visit_status = searchresult.getText( 'custrecord_dsg_site_visit_status' );
					result.custrecord_dsg_tranche_id = searchresult.getValue( 'custrecord_dsg_tranche_id');
					result.custrecord_dsg_site_visit_date = searchresult.getValue('custrecord_dsg_site_visit_date');
					result.custrecord_dsg_inst2_pay_appr_date = searchresult.getValue('custrecord_dsg_inst2_pay_appr_date');
					result.custrecord_dsg_milestone2_pay_appr_date = searchresult.getValue('custrecord_dsg_milestone2_pay_appr_date');
					result.custrecord_dsg_inst3_pay_appr_date = searchresult.getValue('custrecord_dsg_inst3_pay_appr_date');
					result.custrecord_dsg_partner = searchresult.getText('custrecord_dsg_partner');
					result.custrecord_dsg_site_homeowner = searchresult.getValue('custrecord_dsg_site_homeowner');
					result.custrecord_dsg_territory_custom = searchresult.getText( 'custrecord_dsg_territory_custom' );
					result.custrecord_dsg_site_visit_status = searchresult.getText( 'custrecord_dsg_site_visit_status');
					result.custrecord_dsg_comm_date = searchresult.getValue('custrecord_dsg_comm_date');
					result.custrecord_dsg_site_street_1 = searchresult.getValue('custrecord_dsg_site_street_1');
					result.custrecord_dsg_site_city = searchresult.getValue('custrecord_dsg_site_city');
					result.custrecord_dsg_site_state = searchresult.getValue('custrecord_dsg_site_state');
					result.custrecord_dsg_site_zip = searchresult.getValue( 'custrecord_dsg_site_zip' );
					result.custrecord_dsg_pv_module_model = searchresult.getText( 'custrecord_dsg_pv_module_model');
					result.custrecord_dsg_pv_modules_qty = searchresult.getValue('custrecord_dsg_pv_modules_qty');
					result.custrecord_dsg_inverter_mfr_1 = searchresult.getValue('custrecord_dsg_inverter_mfr_1');					
					result.custrecord_dsg_inverter_model_1 = searchresult.getText( 'custrecord_dsg_inverter_model_1');
					result.custrecord_dsg_inverter_qty = searchresult.getValue('custrecord_dsg_inverter_qty');
					result.custrecord_dsg_m3_doc7 = searchresult.getText('custrecord_dsg_m3_doc7');
					result.custrecord_dsg_controller_id = searchresult.getValue('custrecord_dsg_controller_id');
					result.custrecord_dsg_product_type = searchresult.getText( 'custrecord_dsg_product_type' );
					result.custrecord_dsg_site_financing_program = searchresult.getText( 'custrecord_dsg_site_financing_program');
					result.custrecord_dsg_utility_interconnex_date = searchresult.getValue('custrecord_dsg_utility_interconnex_date');		
					result.custrecord_dsg_sys_operation_verify = searchresult.getValue('custrecord_dsg_sys_operation_verify');
					var isHomeOwner = searchresult.getValue('custrecord_dsg_is_homeowner');	
					if(isHomeOwner == 'T')					
						result.custrecord_dsg_is_homeowner = 'Yes';					
					else
						result.custrecord_dsg_is_homeowner = 'No';	
					 	
					result.custrecord_dsg_inverter_model_1 = searchresult.getText('custrecord_dsg_inverter_model_1');		
					result.custrecord_dsg_inverter_model_2 = searchresult.getText('custrecord_dsg_inverter_model_2');		
					result.custrecord_dsg_inverter_model_3 = searchresult.getText( 'custrecord_dsg_inverter_model_3' );		
					result.custrecord_dsg_inverter_mfr_1 = searchresult.getText( 'custrecord_dsg_inverter_mfr_1');		
					result.custrecord_dsg_inverter_mfr_2 = searchresult.getText('custrecord_dsg_inverter_mfr_2');
					result.custrecord_dsg_inverter_mfr_3 = searchresult.getText('custrecord_dsg_inverter_mfr_3');		
					result.custrecord_dsg_enphase_envoy_id = searchresult.getValue('custrecord_dsg_enphase_envoy_id');
					result.custrecord_dsg_pv_module_mfg = searchresult.getValue('custrecord_dsg_pv_module_mfg');		
					result.custrecord_dsg_pv_module_model = searchresult.getValue( 'custrecord_dsg_pv_module_model' );		
					result.custrecord_dsg_pv_module_size = searchresult.getValue( 'custrecord_dsg_pv_module_size');		
					result.custrecord_dsg_monitoring_model = searchresult.getText('custrecord_dsg_monitoring_model');		
					result.custrecord_dsg_monitoring_type = searchresult.getText('custrecord_dsg_monitoring_type');
					result.custrecord_nuvola_tsm = searchresult.getText( 'custrecord_nuvola_tsm');		
					result.custrecord_sales_channel = searchresult.getText('custrecord_sales_channel');		
					result.custrecord_dsg_site_scion_installer = searchresult.getText('custrecord_dsg_site_scion_installer');				
					result.custrecord_se_nr_so_to_ho_or_id = searchresult.getValue(columns[50]);							
					result.custrecord_se_nr_interconnected_so = searchresult.getValue(columns[51]); 
					result.custrecord_dsg_tot_contract_amt = searchresult.getValue('custrecord_dsg_tot_contract_amt');
                    result.custrecord_se_install_status = searchresult.getText('custrecord_se_install_status');
					result.custrecord_se_permitstatus = searchresult.getText('custrecord_se_permitstatus');
                    result.custrecord_dsg_actual_permit_date = searchresult.getValue('custrecord_dsg_actual_permit_date');
                    results.push(result);
				}
				catch (e)
				{
				//	nlapiLogExecution('EMERGENCY','Error in getSiteData ',e);
					continue;
				}
			 }
		}
	    else 
	    {
	    	var error={}; 
	    	error.errormsg = 'No More Results';    
	    	results.push(error);
	    }
    return results;
	
}

// Get Promise Data
function getPromiseData(timestamp,lastRecordId)
{
	var results = [];
	var filters = new Array();
	var columns = new Array();
	filters[0] = new nlobjSearchFilter('lastmodified', null, 'onOrAfter', timestamp);
	columns[0] = new nlobjSearchColumn('custrecord473');
	columns[1] = new nlobjSearchColumn('custrecord445_2');
	columns[2] = new nlobjSearchColumn('custrecord446_2');
	columns[3] = new nlobjSearchColumn('lastmodified' );
	columns[4] = new nlobjSearchColumn('custrecord447_2');//
	columns[5] = new nlobjSearchColumn('custrecord448_2');
	columns[6] = new nlobjSearchColumn('custrecord449_2');
	columns[7] = new nlobjSearchColumn('custrecord450_2');
	columns[8] = new nlobjSearchColumn('custrecord451_2');
	columns[9] = new nlobjSearchColumn('custrecord452');
	columns[10] = new nlobjSearchColumn('custrecord453');
	columns[11] = new nlobjSearchColumn('custrecord454');
	columns[12]= new nlobjSearchColumn('custrecord455');
	columns[13] = new nlobjSearchColumn('custrecord456');
	columns[14] = new nlobjSearchColumn('custrecord470');
	columns[15] = new nlobjSearchColumn('custrecord472');//	
	columns[16] = new nlobjSearchColumn('custrecord474')
	columns[17] = new nlobjSearchColumn('custrecord_dsgref_promise_data')
	columns[18] = new nlobjSearchColumn('internalid')

	columns[3].setSort();
	columns[18].setSort();

	var searchresults = nlapiSearchRecord('customrecord_promisedata', null, filters, columns );
     //   nlapiLogExecution('DEBUG','getPromiseData','searchresults  =' +searchresults.length); 
	if(searchresults) 
	{
		nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
		if(lastRecordId)
		{
			index = getRecordIdIndex(searchresults,lastRecordId,timestamp);
			
		}
		else
		{
			index = 1;
		}
		var search_length = searchresults.length;
		if(search_length>100)
		{
			search_length = 100;
		}
	    for ( var i = index; searchresults != null && i <= search_length; i++ )
		{
	    	try 
	    	   {
	    		
				var result={};
		
				var searchresult  = searchresults[ i ];
				result.recordId = searchresult.getId( );
				result.lastModifiedDate = searchresult.getValue('lastmodified'); 
				result.custrecord473 = searchresult.getText('custrecord473'); // Promise Type		
				result.custrecord445_2 = searchresult.getValue('custrecord445_2'); //Promise (Jan)
				result.custrecord446_2 = searchresult.getValue( 'custrecord446_2' ); //Promise (Feb)
				result.custrecord447_2 = searchresult.getValue( 'custrecord447_2'); //Promise (Mar)
				result.custrecord448_2 = searchresult.getValue('custrecord448_2'); //Promise (Apr)
				result.custrecord449_2 = searchresult.getValue('custrecord449_2'); //Promise (May)
				result.custrecord450_2 = searchresult.getValue('custrecord450_2'); //Promise (Jun)
				result.custrecord451_2 = searchresult.getValue('custrecord451_2');  //Promise (Jul)
				result.custrecord452 = searchresult.getValue('custrecord452');  //Promise (Aug)
				result.custrecord453 = searchresult.getValue('custrecord453'); //Promise (Sep)
				result.custrecord454 = searchresult.getValue( 'custrecord454' ); //Promise (Oct)
				result.custrecord455 = searchresult.getValue( 'custrecord455'); //Promise (Nov)
				result.custrecord456 = searchresult.getValue('custrecord456');  //Promise (Dec)
				result.custrecord470 = searchresult.getValue('custrecord470'); // Degradation Rate
				result.custrecord472 = searchresult.getValue('custrecord472'); //Expected to Promise Ratio	
				result.homeowner_id = searchresult.getValue('custrecord474'); // HomeOwner
				result.site_id = searchresult.getValue('custrecord_dsgref_promise_data'); //Expected to Promise Ratio
				results.push(result);
			}
			catch (e)
			{
				nlapiLogExecution('EMERGENCY','Error in getPromiseData ',e);
				continue;
			}
		}
	 }
	else
	{
		var error={}; 
    	error.errormsg = 'No More Results';    
    	results.push(error);
	}
	return results;

}


// Get Array Design Info Data..
function getArrayInfoData(timestamp,lastRecordId)
{	
	var results = [];
	var filters = new Array();
	var columns = new Array();
    filters[0] = new nlobjSearchFilter('lastmodified', null, 'onOrAfter', timestamp);
	columns[0] = new nlobjSearchColumn('custrecord206');
	columns[1] = new nlobjSearchColumn('custrecord208');
	columns[2] = new nlobjSearchColumn('custrecord209');
	columns[3] = new nlobjSearchColumn('custrecord210' );
	columns[4] = new nlobjSearchColumn('custrecord212');//
	columns[5] = new nlobjSearchColumn('custrecord213');
	columns[6] = new nlobjSearchColumn('custrecord214');
	columns[7] = new nlobjSearchColumn('custrecord215');
	columns[8] = new nlobjSearchColumn('custrecord216');
	columns[9] = new nlobjSearchColumn('custrecord217');
	columns[10] = new nlobjSearchColumn('custrecord218');
	columns[11] = new nlobjSearchColumn('custrecord219');
	columns[12]= new nlobjSearchColumn('custrecord220');
	columns[13] = new nlobjSearchColumn('custrecord221');
	columns[14] = new nlobjSearchColumn('custrecord222');
	columns[15] = new nlobjSearchColumn('custrecord223');
	columns[16] = new nlobjSearchColumn('lastmodified' );//
	columns[17] = new nlobjSearchColumn('custrecord205');
	columns[18] = new nlobjSearchColumn('custrecord_dsgref_array_design' );

	columns[19] = new nlobjSearchColumn('internalid')

	columns[16].setSort();
	columns[19].setSort();

	var searchresults = nlapiSearchRecord('customrecord210', null, filters, columns );
	if(searchresults) 
	{
		nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
		if(lastRecordId)
		{
			index = getRecordIdIndex(searchresults,lastRecordId,timestamp);
			
		}
		else
		{
			index = 1;
		}
		var search_length = searchresults.length;
		if(search_length>100)
		{
			search_length = 100;
		}
		for ( var i = index; searchresults != null && i <= search_length; i++ )
			{
				try 
				{
					var result={};
			
					var searchresult  = searchresults[ i ];
					result.recordId = searchresult.getId( );		
					result.lastModifiedDate = searchresult.getValue('lastmodified');
					result.custrecord206 = searchresult.getValue('custrecord206'); // Array #
					result.custrecord208 = searchresult.getValue('custrecord208'); //THM Module Qty/Roof
					result.custrecord209 = searchresult.getValue( 'custrecord209' ); //  Roof Tilt (degrees)
					result.custrecord210 = searchresult.getValue( 'custrecord210'); // Orientation
					result.custrecord212 = searchresult.getValue('custrecord212'); // Solar Access (Jan)
					result.custrecord213 = searchresult.getValue('custrecord213'); // Solar Access (Feb)
					result.custrecord214 = searchresult.getValue('custrecord214'); // Solar Access (Mar)
					result.custrecord215 = searchresult.getValue('custrecord215');   // Solar Access (Apr)
					result.custrecord216 = searchresult.getValue('custrecord216');   // Solar Access (May)
					result.custrecord217 = searchresult.getValue('custrecord217'); // Solar Access (Jun)
					result.custrecord218 = searchresult.getValue( 'custrecord218' ); // Solar Access (Jul)
					result.custrecord219 = searchresult.getValue( 'custrecord219'); // Solar Access (Aug)
					result.custrecord220 = searchresult.getValue('custrecord220'); // Solar Access (Sep)
					result.custrecord221 = searchresult.getValue('custrecord221'); // Solar Access (Oct)						
					result.custrecord222 = searchresult.getValue('custrecord222'); // Solar Access (Nov)
					result.custrecord223 = searchresult.getValue('custrecord223'); // Solar Access (Dec)
					result.homeowner_id = searchresult.getValue('custrecord205'); // Solar Access (Nov)
					result.site_id = searchresult.getValue('custrecord_dsgref_array_design'); // Solar Access (Dec)						
					results.push(result);
					}
				catch (e)
				{
					nlapiLogExecution('EMERGENCY','Error in getArrayInfoData ',e);
					continue;
				}
			}
	}
	else 
	{
		var error={}; 
    	error.errormsg = 'No More Results';    
    	results.push(error);
	}
	return results;
	
}

//Get Case Data
function getCaseData(timestamp,lastRecordId)
{	
		var results = [];
		var filters = new Array();
		var columns = new Array();
	    filters[0] = new nlobjSearchFilter('lastmodifieddate', null, 'onOrAfter', timestamp);
		columns[0] = new nlobjSearchColumn('casenumber');
		columns[1] = new nlobjSearchColumn('title');
		columns[2] = new nlobjSearchColumn('status');
		columns[3] = new nlobjSearchColumn('lastmodifieddate');
		columns[4] = new nlobjSearchColumn('startdate');//
		columns[5] = new nlobjSearchColumn('company');
		//columns[6] = new nlobjSearchColumn('custentity_contract_canceled_date');
	
		columns[6] = new nlobjSearchColumn('internalid')

		columns[3].setSort();
		columns[6].setSort();
	
		var searchresults = nlapiSearchRecord('supportcase', null, filters, columns );
		if(searchresults) 
		{
			nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
			if(lastRecordId)
			{
				index = getRecordIdIndex(searchresults,lastRecordId,timestamp);
				
			}
			else
			{
				index = 1;
			}
			var search_length = searchresults.length;
			if(search_length>100)
			{
				search_length = 100;
			}
			for ( var i = index; searchresults != null && i <= search_length; i++ )
			{
				try
				{
					var result={};
			
					var searchresult  = searchresults[ i ];
					result.recordId = searchresult.getId( );
					result.lastModifiedDate = searchresult.getValue('lastmodifieddate');
					result.casenumber = searchresult.getValue('casenumber');		
					result.title = searchresult.getValue('title');
					result.status = searchresult.getText( 'status' );
					result.startdate = searchresult.getValue( 'startdate');
					var company = searchresult.getValue('company');
					nlapiLogExecution('DEBUG','getPromiseData','company  =' +company); 
					result.homeowner_id = company;
					var site_id = nlapiLookupField('customer',company,'custentity_dsg_lead_site_id');			
					result.site_id = site_id;
					
					results.push(result);
				}
				catch (e)
				{
					nlapiLogExecution('EMERGENCY','Error in getCaseData ',e);
					continue;
				}
			}
		}
		else {
			var error={}; 
	    	error.errormsg = 'No More Results';    
	    	results.push(error);
		}
		return results;
}	

//  Get Array Design Info Data.. 
function getSalesOrderData(timestamp,lastRecordId)
{	
	var results = [];
	var filters = new Array();
	var columns = new Array();
    filters[0] = new nlobjSearchFilter('lastmodifieddate', null, 'onOrAfter', timestamp);
    filters[1] = new nlobjSearchFilter( 'mainline', null, 'is', 'T');
	columns[0] = new nlobjSearchColumn('custbody20');
	columns[1] = new nlobjSearchColumn('number');
	columns[2] = new nlobjSearchColumn('datecreated');
	columns[3] = new nlobjSearchColumn('lastmodifieddate');
	columns[4] = new nlobjSearchColumn('custbody_pvtotalkwordered');//
	columns[5] = new nlobjSearchColumn('entity');
	columns[6] = new nlobjSearchColumn('custbody_dsg_home_site_id');	
	columns[7] = new nlobjSearchColumn('status'); 
	columns[8] = new nlobjSearchColumn('custbody_currentpromiseddate');
	columns[9] = new nlobjSearchColumn('statusref');
	columns[10] = new nlobjSearchColumn('internalid')
	columns[3].setSort();
	columns[10].setSort();

	var searchresults = nlapiSearchRecord('salesorder', null, filters, columns );
	if(searchresults) 
	{
		nlapiLogExecution('DEBUG','searchresults length ','length : '+searchresults.length);
		if(lastRecordId)
		{
			index = getRecordIdIndex(searchresults,lastRecordId,timestamp);
			
		}
		else
		{
			index = 1;
		}
		var search_length = searchresults.length;
		if(search_length>100)
		{
			search_length = 100;
		}
		for ( var i = index; searchresults != null && i <= search_length; i++ )
		{
			try
			{
				var result={};		
				var searchresult  = searchresults[ i ];
				result.recordId = searchresult.getId( );				
				result.custbody20 = searchresult.getValue('custbody20'); // Actual Delivery Date		 
				result.number = searchresult.getValue('number');
				result.datecreated = searchresult.getValue( 'datecreated' );
				result.custbody_pvtotalkwordered = searchresult.getValue('custbody_pvtotalkwordered');
				result.lastModifiedDate = searchresult.getValue('lastmodifieddate');
				result.homeowner_id = searchresult.getValue('entity');							
				result.site_id = searchresult.getValue('custbody_dsg_home_site_id');
				result.orderstatus = searchresult.getText('statusref');
				result.custbody_currentpromiseddate = searchresult.getValue('custbody_currentpromiseddate');
				results.push(result);
			}
			catch (e)
			{
				nlapiLogExecution('EMERGENCY','Error in getSalesOrderData ',e);
				continue;
			}
		}
	}
	else {
		var error={}; 
    	error.errormsg = 'No More Results';    
    	results.push(error);
	}
	return results;

}
// fetch the index of last record sent to Redshift in last call for a given timestamp ..
function getRecordIdIndex(searchresults,lastRecordId,timestamp)
{
	var index = 1;
	for ( var i = 1; searchresults != null && i <= searchresults.length; i++ )
	{
		try 
		{ 
			//nlapiLogExecution('DEBUG','getRecordIdIndex ','timestamp : '+timestamp);
			//nlapiLogExecution('DEBUG','getRecordIdIndex ','index : '+i);
			var searchresult  = searchresults[ i ];
			//nlapiLogExecution('DEBUG','getRecordIdIndex ','searchresult : '+searchresult);
			var recordinternalId = searchresult.getId( );
			//nlapiLogExecution('DEBUG','getRecordIdIndex ','recordinternalId : '+recordinternalId);
			var stamp = searchresult.getValue('lastmodified');
			//nlapiLogExecution('DEBUG','getRecordIdIndex ','stamp : '+stamp);
			if(stamp == timestamp)
			{
				if(recordinternalId >= lastRecordId)
				{
					nlapiLogExecution('DEBUG','getRecordIdIndex','Matched recordinternalId : '+recordinternalId);
					index = i;
					nlapiLogExecution('DEBUG','index count','Matched index count : '+index);
					break;	
				}	
			}
		/*	else 
			{
				index = 0;
			}
		*/
		}
		catch (e)
		{
			//nlapiLogExecution('EMERGENCY','Error in getSiteData ',e);
			continue;
		}
	}
	nlapiLogExecution('DEBUG','getRecordIdIndex','Return Index : '+index);
	return index;
}