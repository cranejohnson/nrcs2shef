<?php
	// This script uses php SoapClient to pull data from NRCS web service.  It accepts a date arg to pull historical values
	// The script first pulls a list of Snow Course stations containing SV data, then pulls SV data from each station.
	// If no SV data is available it checks to see if there is a colocated SnowTel station.  If found it will attempt to
	// pull SV data.  If that's not available it will pull SD data.  The script uses mapping in stnListSnowCrs.php to get
	// the NWSID that matches the name of the station.  SHEF data is then generated and sent to CHPS.
	// Business logic like file names and additional data formats like sweWeb are taken from legacy code and may no longer be needed
	
	// The second part of the script pull data from a Canadian BC ftp site and finds all stations that have a NWSID match
	// and creates SHEF for CHPS
        //
        //  usage:  'php nrcs2shef.php 202102 benjamin.johnson@noaa.gov'  runs the script for Feb 2021 and emails results to Crane


	include 'stnListSnowCrs.php';  //contains $stationList array with names as keys and NWSID as value
        error_reporting(0);
        $mailer = "/var/www/html/tools/PHPMailer/PHPMailerAutoload.php";
	if (file_exists($mailer)) {
		require $mailer;
	}	


	date_default_timezone_set('UTC');
	$unixDateNow = date_format(new DateTime(), 'U') * 1000;
	$chpsShef = array();
	$qYear = date("Y");  // 2021,2020
	$monthDay = date("md");
	$qMonth = date("m");   // 01,02
	$yearMon = date("Ym");
	$lastDayOfMonth = date("t");  
	
	//Order to select swe measurments from co-located sites
	$pickFirst = ['SNOW','SNTL','MSNT','COOP'];
	$summaryStats = array();
	$files = array();
	$emailList = array();

	$summaryStats['Sites in Station List'] = count($stationList);


	
	if($argv){
		foreach ($argv as $arg){
			if(strpos($arg, '@')) $emailList[] = $arg;
			if (preg_match("/\d{6}/",$arg)){
				$qYear = substr($arg,0,4);
				$qMonth = substr($arg,4,2);
			}
		}
	}	
	

	$fp = fopen('nrcsRaw_'.$qYear.$qMonth.'.csv', 'w'); 
	$files[]= 'nrcsRaw_'.$qYear.$qMonth.'.csv';
		
	$summaryStats['Data For'] = date('M Y', strtotime($qMonth."/01/".$qYear));
	$summaryStats['Script RunTime'] = date('D M j G:i T Y', time());
	
	
	
	//Create the client object
	$soapclient = new SoapClient('http://www.wcc.nrcs.usda.gov/awdbWebService/services?WSDL', array('connection_timeout'=> 120, 'exceptions' => 0));
	
    	//Get a list of stations with SWE Data
	$getStnsParams = array('stateCds' => array('AK','YK','BC'), 'networkCds' => array('SNOW','SNTL','MSNT','COOP'),'logicalAnd' => '1', 'elementCd' => 'WTEQ' );
	$stnResp = $soapclient->getStations($getStnsParams);
	
	//Setup query to get selected station meta data
	$stnMetaParams = array('stationTriplets' => $stnResp->return);
	$stnMetaResp = $soapclient->getStationMetadataMultiple($stnMetaParams);

    	//Sort MetaData Array in Alphabetical Order by Name
	usort($stnMetaResp->return, "cmp");
	
	
	
	// Build SWE queries
	$sweParams = array('stationTriplets' => $stnResp->return, 'elementCd' => 'WTEQ', 'ordinal' => '1',
	'duration' => 'SEMIMONTHLY', 'beginDate' => $qYear.'-'.$qMonth.'-01', 'endDate' => $qYear.'-'.$qMonth.'-'.$lastDayOfMonth, 'getFlags' => '0');

    	// Get SWE data for all of the stations and put the data into a array with key value staionTriplet
	$sweResp = $soapclient->getData($sweParams);
	$sweValues = array();
		
	foreach($sweResp->return as $sweVal){
		$value = array();
		if($sweVal->values[0]){
			$value['date'] = $sweVal->collectionDates[0];
			$value['swe'] = $sweVal->values[0];
		}else{
			$value['date'] = '';
			$value['swe'] = -9.99;
		}					
		$sweValues[$sweVal->stationTriplet] = $value;	
	}


	
    	// Build Normal queries
	$sweNormParams = array('stationTriplets' => $stnResp->return, 'elementCd' => 'WTEQ', 'duration' => 'SEMIMONTHLY',
	'beginMonth' => $qMonth, 'beginDay' => '01', 'endMonth' => $qMonth, 'endDay' => $lastDayOfMonth, 'getFlags' => '0',
	"centralTendencyType" => "NORMAL");

	$normResp = $soapclient->getCentralTendencyData($sweNormParams);
	$normalValues = array();
	
	for($i=0; $i<count($stnResp->return);$i++){
		$norm['type'] = $normResp->return[$i]->centralTendencyType;
		$norm['val'] = $normResp->return[$i]->values[0];
		$normalValues[$stnResp->return[$i]] = $norm;
	}
		

	$stationData = array();
	$shefStations = array();	

    	//Cycle through all the sites and add a few more fields to the site objects (swe data etc)
	foreach($stnMetaResp->return as $station){
	
		//Add swe data 
		$station->swe = round($sweValues[$station->stationTriplet]['swe'],2);
		$station->obsDate = $sweValues[$station->stationTriplet]['date'];
		
        	//Add the nws shef id from the listing file
		$station->nwsShefId = $stationList[$station->name];
		
		if($station->nwsShefId){
			$station->id = $station->nwsShefId;
		}else{
			$station->id = $station->shefId;
		}
		
		
		//add the normal values either median or average to the object
		$type = $normalValues[$station->stationTriplet]['type'];
		if($type) $station->$type = $normalValues[$station->stationTriplet]['val'];
		$stationData[$station->stationTriplet] = $station;

		//Calculate % of normal if there is and average and current data
		if($station->swe > -9.99 && $normalValues[$station->stationTriplet]['val'] ){
                    $station->percentNormal = round(($station->swe/$normalValues[$station->stationTriplet]['val'])*100);
		}    

		$trip = explode(":",$station->stationTriplet);
		$station->network = $trip[2];
		$station->state = $trip[1];

		if(strtotime($station->endDate) > time()) {
			$station->active = 'true';
			$summaryStats['NRCS Status']['Active sites']++;
		}else{
			$station->active = 'false';
			$summaryStats['NRCS Status']['Sites not active']++;
			}		
		
		
		if($trip[1] == "AK" & $station->active == 'true') $summaryStats['NRCS']['region']['AK']['Active']++;
		if($trip[1] == "BC" & $station->active == 'true') $summaryStats['NRCS']['region']['BC']['Active']++;
		if($trip[1] == "YK" & $station->active == 'true') $summaryStats['NRCS']['region']['YK']['Active']++;

		if($trip[1] == "AK" & $station->swe > -9.99) $summaryStats['NRCS']['region']['AK']['With Data']++;
		if($trip[1] == "BC" & $station->swe > -9.99) $summaryStats['NRCS']['region']['BC']['With Data']++;
		if($trip[1] == "YK" & $station->swe > -9.99) $summaryStats['NRCS']['region']['YK']['With Data']++;

		if($trip[2] == "SNTL" & $station->swe > -9.99) $summaryStats['NRCS']['network']['Snotel']['With Data']++;
		if($trip[2] == "SNOW" & $station->swe > -9.99) $summaryStats['NRCS']['network']['Snow Course']['With Data']++;
		if($trip[2] == "COOP" & $station->swe > -9.99) $summaryStats['NRCS']['network']['COOP']['With Data']++;
		if($trip[2] == "MSNT" & $station->swe > -9.99) $summaryStats['NRCS']['network']['Manual Snotel']['With Data']++;



		//Assign swe values to shef ids, use NWS shef id if it exists and if not use NRCS shef id		
		if($station->shefId || $station->nwsShefId){
			if($station->nwsShefId){		    
				$shefStations[$station->nwsShefId][$station->network] = $station->swe;
				$shefStations[$station->nwsShefId]['idFrom'] = 'NWS';
			}else{
				$shefStations[$station->shefId][$station->network] = $station->swe;
				$shefStations[$station->shefId]['idFrom'] = 'NRCS';
			}	
		}
		
	
		$summaryStats['NRCS_sitesNotEncoded'] = array();
		if(strlen($station->nwsShefId)>0 ){
			if($station->nwsShefId <> $station->shefId){
				if($station->active == 'true'){
					$summaryStats['Errors']['Active Stations'][] = "Check {$station->stationTriplet},{$station->name},NWS File Shef ID:{$station->nwsShefId},NRCS Shef ID:{$station->shefId}\n";
				}else{
					$summaryStats['Errors']['Sites not Active'][] = "Check {$station->stationTriplet},{$station->name},NWS File Shef ID:{$station->nwsShefId},NRCS Shef ID:{$station->shefId}\n";
				}
			}
		}
	}


	
	ksort($shefStations);
	foreach($shefStations as $nwsid => $station){
		$sweData = -9999;
		$typeUsed = "";
		foreach($pickFirst as $type){
                        if(!isset($station[$type])) continue;
			if($station[$type] > -9){
				$sweData = $station[$type];
				$typeUsed = $type;
				if($type == 'SNOW') $typeUsed = "Snow Course";
				if($type == 'SNTL') $typeUsed = "Snotel";
				break;
			}
		}
	
		if($sweData > -9.99 && $station['idFrom'] == 'NWS'){
			foreach($stnMetaResp->return as $station){
				if(($station->id == $nwsid) && ($station->network == $typeUsed)){
					$station->shefEncoding = 'data';
				} 
			}		                     
			$chpsShef[] = ".A ".$nwsid." ".$qYear.$qMonth."01 Z DH00/DC".date('Ymd')."0000/SWIRV ".sprintf("%-8s",$sweData)." : ".$typeUsed."\n";
			if (($key = array_search($nwsid, $stationList)) !== false) {
    			unset($stationList[$key]);
			}
			
		}elseif($sweData > -9.99){
			$chpsMaybeShef[] = ".A ".$nwsid." ".$qYear.$qMonth."01 Z DH00/DC".date('Ymd')."0000/SWIRV ".sprintf("%-8s",$sweData)." : ".$typeUsed."\n";	
		}
	}	
		
	$summaryStats['NRCS_sitesNotEncoded'] = $stationList;	
	foreach($stationList as $name => $lid){
		$chpsSWEShefMissing[] = ".A ".$lid." ".$qYear.$qMonth."01 Z DH00/DC".date('Ymd')."0000/SWIRV -9.99\n";
		foreach($stnMetaResp->return as $station){
			if($station->id == $lid){
				$station->shefEncoding = 'missing';
			} 
		}		                     

	}
	
	
	$summaryStats['numShefEncoded'] = count($chpsShef);
	$summaryStats['numShefMissing'] = count($chpsSWEShefMissing);
	
	
	
	//Put everything into a csv file for viewing in excel
	createCSV($fp,$stnMetaResp->return);
	
	fclose($fp);
	$files[] = "nrcs2shef_chps_".$qMonth.$qYear.".txt";



	
	//Get additional obs from env.gov.bc.ca
	//mss_report.csv is ALL CAPS so need to create an uppercase to title case map
	$keys=array_keys($stationList);
	$map=array();
	$bcShef = array();
	$summaryStats['BC']['numsites'] = -1;
	$summaryStats['BC']['numsitesIncluded'] = 0;
	foreach($keys as $key)
	{
		 $map[strtoupper($key)]=$key;
	}
	//array to store obs to select latest
	$latestObsIndex = array();
	$datafile = file("ftp://ftp.env.gov.bc.ca/pub/outgoing/EMS/MSS/mss_report.csv");
	$shefData = array();
	//find lines that have latest obs for stations found in $stationList array
	foreach($datafile as $index => $line){
	    $summaryStats['BC']['numsites']++;
		$parts = explode(",",$line);
		if (array_key_exists($parts[0],$map)){
		    $summaryStats['BC']['numsitesIncluded']++;

			$lid = $stationList[$map[$parts[0]]];
			$obDate = $parts[3];
			if (!isset($obs[$lid])){
				$latestObsIndex[$lid] = array("","");
			}
			if ($obDate > $latestObsIndex[$lid][0]){
				$latestObsIndex[$lid] = array($obDate,$index);
			}
		}
	}
	//create SHEF
	foreach($latestObsIndex as $lid => $data){
		$parts = explode(",",$datafile[$data[1]]);
		$obDate = $parts[3];
		$dateParts = explode("/",$obDate);
		$cayear = $dateParts[0];
		$camon = $dateParts[1];
		$caday = $dateParts[2];
		$sd_cm = $parts[4];
		$sd = round($sd_cm * 0.393701,1);
		$swe_mm = $parts[5];
		$swe = round($swe_mm * 0.0393701,1);
		$precentNorm = $parts[9];
		$survPeriod = $parts[11];
		$norm_mm = $parts[12];
		$bcShef[] = ".A ".$nwsid." ".$cayear.$camon.$caday." Z DH00/DC".$cayear.$camon.$caday."0000/SDIRV ".$swe."\n";
		
		if ($norm_mm != ''){
			$norm = round($norm_mm * 0.0393701,1);
			$chpsShef[] = ".A ".$lid." ".$cayear.$camon.$caday." Z DH00/DC".$cayear.$camon.$caday."0000/SWIPV ".$precentNorm."\n";
		}
	}
	
	file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", $chpsShef);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", ":Stations below are from BC \n",FILE_APPEND);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", $bcShef,FILE_APPEND);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", ":StnList NRCS sites below were missing \n",FILE_APPEND);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", $chpsSWEShefMissing,FILE_APPEND);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", ":NRCS sites below were not in stnList\n",FILE_APPEND);
	#file_put_contents("nrcs2shef_chps_".$qMonth.$qYear.".txt", $chpsMaybeShef,FILE_APPEND);

	if (file_exists($mailer)) {
		sendEmail($summaryStats,$files,$emailList);
		echo "Email with results sent to:\n";
		foreach($emailList as $email){
			echo "   $email\n";
		}
	}else{
		print_r($summaryStats);
	}	



	###Uncomment the line below to push to CHPS
	
	#exec('/usr/bin/scp /usr/local/apps/scripts/nrcs2shef/nrcs2shef_chps_*.txt ldad@ls1-acr:/data/Incoming');

    #######
    #
    # Functions
    #
    #######
	function sendEmail($summary,$files,$recipients = array()){
		global $startTime;
		
 
#		$recipients[] = 'benjamin.johnson@noaa.gov';

		$message = json_encode($summary, JSON_PRETTY_PRINT);


		$mail = new PHPMailer;
	
		$mail->FromName = 'nws.ar.aprfc';

		foreach($recipients as $email)
		{
			$mail->AddAddress($email,$email);
		}
		$mail->Subject = "nrcs2shef";
		$mail->Body = $message;
		foreach($files as $file){
			$mail->addAttachment($file);
		}
		if(!$mail->send()) {
			echo 'Message could not be sent.';
			echo 'Mailer Error: ' . $mail->ErrorInfo;
		} else {
			echo "Results sent by email\n";


		}
		return $recipients;

	}




	function cmp($a, $b) {
		return strcmp($a->name, $b->name);
	}
	
	function createCSV($fp,$data){
		$csv = "";
		$headers = ['name','active','state','latitude','longitude','shefId','nwsShefId','shefEncoding','network','stationTriplet','beginDate','endDate','obsDate','swe','percentNormal','AVERAGE','MEDIAN'];
		fputcsv($fp, $headers); 
		foreach($data as $d){
			foreach($headers as $h){
				if(property_exists($d,$h)) {
				   fwrite($fp, '"'.$d->$h.'",');			
				}else{
				   fwrite($fp,",");
				}   
			}
			fwrite($fp,"\n");
		}				
	}
	
?>
