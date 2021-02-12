<?php
	// This script uses php SoapClient to pull data from NRCS web service.  It accepts a date arg to pull historical values
	// The script first pulls a list of Snow Course stations containing SV data, then pulls SV data from each station.
	// If no SV data is available it checks to see if there is a colocated SnowTel station.  If found it will attempt to
	// pull SV data.  If that's not available it will pull SD data.  The script uses mapping in stnListSnowCrs.php to get
	// the NWSID that matches the name of the station.  SHEF data is then generated and sent to CHPS.
	// Business logic like file names and additional data formats like sweWeb are taken from legacy code and may no longer be needed
	
	// The second part of the script pull data from a Canadian BC ftp site and finds all stations that have a NWSID match
	// and creates SHEF for CHPS

	include 'stnListSnowCrs.php';  //contains $stationList array with names as keys and NWSID as value
	require_once "/var/www/html/tools/PHPMailer/PHPMailerAutoload.php";


	date_default_timezone_set('UTC');
	$unixDateNow = date_format(new DateTime(), 'U') * 1000;
	$chpsShef = array();
	$lidList = array();
	$year = date("Y");
	$yr = date("y");
	$monthDay = date("md");
	$mon = date("m");
	$yearMon = date("Ym");
	$lastDayOfMonth = date("t");
	$reportFile = "";
	$pickFirst = ['SNTL','SNOW','MSNT','COOP'];
	$summaryStats = array();
	$files = array();






	function sendEmail($summary,$files,$overRideEmail = false){
		global $startTime;
		$recipients = array(
		   //'jessica.cherry@noaa.gov' => 'Jessie'
		   );
 
		//If an overRideEmail is provided use that
		if($overRideEmail){
			$recipients = $overRideEmail;
		}


		$message = json_encode($summary, JSON_PRETTY_PRINT);


		$mail = new PHPMailer;
	
		$mail->FromName = 'nws.ar.aprfc';
		$mail->addAddress('benjamin.johnson@noaa.gov','Crane');

		foreach($recipients as $email => $name)
		{
			$mail->AddAddress($email, $name);
		}
		$mail->Subject = "nrcs2shef";
		$mail->Body = $message;
		foreach($files as $file){
			$mail->addAttachment($file);
		}
		if(!$mail->send()) {
			echo 'Message could not be sent.';
			echo 'Mailer Error: ' . $mail->ErrorInfo;
			$logger->log("Failed to send Rating Email: ".$mail->ErrorInfo,PEAR_LOG_INFO);
		} else {
			 $logger->log("Rating update email sent.",PEAR_LOG_INFO);

		}

	}




	function cmp($a, $b) {
		return strcmp($a->name, $b->name);
	}
	
	function createCSV($fp,$data){
		$csv = "";
		$headers = ['name','active','latitude','longitude','shefId','nwsShefId','network','stationTriplet','beginDate','endDate','obsDate','swe','AVERAGE','MEDIAN'];
		fputcsv($fp, $headers); 
		foreach($data as $d){
			foreach($headers as $h){
				fwrite($fp, '"'.$d->$h.'",');			
			}
			fwrite($fp,"\n");
		}				
	}
	

	if (isset($argv[1])){
		$dateToQuery = $argv[1];
		if (!preg_match("/\d{6}/",$dateToQuery)){
			print "If an argument is provided it should be a date YYYYMM\n";
			exit;
		}
		$qYear = substr($dateToQuery,0,4);
		$qMonth = substr($dateToQuery,4,2);
	}else{
		$qYear = $year;
		$qMonth = $mon;
	}

	$fp = fopen('nrcsRaw'.$qYear.$qMonth.'.csv', 'w'); 
	$files[]= 'nrcsRaw'.$qYear.$qMonth.'.csv';
		
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
	
		$station->swe = $sweValues[$station->stationTriplet]['swe'];
		$station->obsDate = $sweValues[$station->stationTriplet]['date'];
		
		$station->nwsShefId = $stationList[$station->name];
		$type = $normalValues[$station->stationTriplet]['type'];
		if($type) $station->$type = $normalValues[$station->stationTriplet]['val'];
		$stationData[$station->stationTriplet] = $station;
		$trip = explode(":",$station->stationTriplet);
		$station->network = $trip[2];
		if(strtotime($station->endDate) > time()) {
			$station->active = 'true';
			$summaryStats['Status']['Active sites']++;
		}else{
			$station->active = 'false';
			$summaryStats['Status']['Sites not active']++;
			}		
		
		if($trip[1] == "AK" & $station->active == 'true') $summaryStats['Number of Sites']['region']['AK']['Active']++;
		if($trip[1] == "BC" & $station->active == 'true') $summaryStats['Number of Sites']['region']['BC']['Active']++;
		if($trip[1] == "YK" & $station->active == 'true') $summaryStats['Number of Sites']['region']['YK']['Active']++;

		if($trip[1] == "AK" & $station->swe > -9.99) $summaryStats['Number of Sites']['region']['AK']['With Data']++;
		if($trip[1] == "BC" & $station->swe > -9.99) $summaryStats['Number of Sites']['region']['BC']['With Data']++;
		if($trip[1] == "YK" & $station->swe > -9.99) $summaryStats['Number of Sites']['region']['YK']['With Data']++;

		if($trip[2] == "SNTL" & $station->swe > -9.99) $summaryStats['Number of Sites']['network']['Snotel']['With Data']++;
		if($trip[2] == "SNOW" & $station->swe > -9.99) $summaryStats['Number of Sites']['network']['Snow Course']['With Data']++;
		if($trip[2] == "COOP" & $station->swe > -9.99) $summaryStats['Number of Sites']['network']['COOP']['With Data']++;
		if($trip[2] == "MSNT" & $station->swe > -9.99) $summaryStats['Number of Sites']['network']['Manual Snotel']['With Data']++;



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
		foreach($pickFirst as $type){
			if($station[$type] > -9){
				$sweData = $station[$type];
				continue;
			}
		}
	
		if($sweData > -9999){	
			$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SWIRV ".$sweData."   :".$station['idFrom']." shefId\n";
		}
	}	
	
	
	
	file_put_contents("nrcs2shef_chps_".$mon.$yr.".txt", $chpsShef);
	print_r($summaryStats);
    //Put everything into a csv file for viewing in excel
	createCSV($fp,$stnMetaResp->return);
	
	fclose($fp);
	$files[] = "nrcs2shef_chps_".$mon.$yr.".txt";
	//sendEmail($summaryStats,$files);
	
	
	exit();

 	// Create a GeoJSON Object to store data.  This is not required for the SHEF but for potential
	// use in GIS / web mapping applications
	$geoData = new stdClass();
	$geoData->type = "FeatureCollection";
	$geoData->features = array();
	
	
	
	


	


	foreach ($stnResp->return as $station) {
		$stnTrip = explode(":", $station);   //Explodes in [NRCSID,State,Type]
		foreach ($stnMetaResp->return as $metaObj) {
			if ($metaObj->stationTriplet == $station){
				if (strstr($metaObj->name, "Disc") || !isset($stationList[$metaObj->name])) {  //Disc or Discontinued or station is not on our list
					continue 2;  //break out of this foreach and continue in the outer foreach
				}else{
					$nwsid = $stationList[$metaObj->name];
					$name = $metaObj->name;
					$elev = $metaObj->elevation;
					$lat = $metaObj->latitude;
					$lon = $metaObj->longitude;
					if ($stnTrip[1] == 'AK'){
						$huc = $metaObj->huc;
					}
					// Build SWE queries
					$sweParams = array('stationTriplets' => $station, 'elementCd' => 'WTEQ', 'ordinal' => '1',
					'duration' => 'SEMIMONTHLY', 'beginDate' => $qYear.'-'.$qMonth.'-01', 'endDate' => $qYear.'-'.$qMonth.'-'.$lastDayOfMonth, 'getFlags' => '0');
					$sweNormParams = array('stationTriplets' => $station, 'elementCd' => 'WTEQ', 'duration' => 'SEMIMONTHLY',
					 'beginMonth' => $qMonth, 'beginDay' => '01', 'endMonth' => $qMonth, 'endDay' => $lastDayOfMonth, 'getFlags' => '0',
					 "centralTendencyType" => "NORMAL");
				}		
			}
		}
		

		//default to missing
		$sweData = -9.99;
		$sweNormData = -9.99;
		$sdData = -9.99;
		if (isset($sweResp->return->values)){
			//dates and obs are in separate array objects
			$latestIndex = count($sweResp->return->values) - 2;
			//date only used for getting corresponding normal value
			$sweDate = $sweResp->return->collectionDates[$latestIndex];
			$sweData = $sweResp->return->values[$latestIndex];
			
			$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SWIRV ".$sweData."\n";
			
			//print $station . "\n".$name."\nSWE ".$snData . "\n";
			$sweNormResp = $soapclient->getCentralTendencyData($sweNormParams);
			//print "SwenormResp\n";
			//print_r($sweNormResp);
			if (is_soap_fault($sweNormResp)){
				print "Got an error connecting to server.  Retrying...";
				sleep(1);
				$sweNormResp = $soapclient->getCentralTendencyData($sweNormParams);
				if (is_soap_fault($sweNormResp)){
					print "Two errors in a row for SwenormResp.  Exiting.";
					exit;
				}
			}
			if (isset($sweNormResp->return->values)){
				$sweNormData = $sweNormResp->return->values[0];
				//print "Norm SWE ".$normData . "\n";
			}
		}elseif($stnTrip[1] == 'AK'){
			//print "No Snow Course SWE so look for a colocated SnowTel site only in AK\n";
			$sntlParams = array('hucs' => $huc, 'networkCds' => 'SNTL', 'logicalAnd' => '1');
			$sntlResp = $soapclient->getStations($sntlParams);
			//print "sntlResp\n";
			//print_r($sntlResp);
			if (is_soap_fault($sntlResp)){
				print "Got an error connecting to server.  Retrying...";
				sleep(1);
				$sntlResp = $soapclient->getStations($sntlParams);
				if (is_soap_fault($sntlResp)){
					print "Two errors in a row for sntlResp.  Exiting.";
					exit;
				}
			}
			if (isset($sntlResp->return)){
				//print "Got a colocated SnowTel\n";
				$sntlTrip = $sntlResp->return;
				$sntlSweParams = array('stationTriplets' => $sntlTrip, 'elementCd' => 'WTEQ', 'ordinal' => '1',
						'duration' => 'DAILY', 'beginDate' => $qYear.'-'.$qMonth.'-01', 'endDate' => $qYear.'-'.$qMonth.'-01', 'getFlags' => '0');
				$sntlSweResp = $soapclient->getData($sntlSweParams);
				//print "sntlSweResp\n";
				//print_r($sntlSweResp);
				if (is_soap_fault($sntlSweResp)){
					print "Got an error connecting to server.  Retrying...";
					sleep(1);
					$sntlSweResp = $soapclient->getData($sntlSweParams);
					if (is_soap_fault($sntlSweResp)){
						print "Two errors in a row for sntlSweResp.  Exiting.";
						exit;
					}
				}
				
				if (!isset($sntlSweResp->return->values)){
					//print "...but it has no SWE data.\n";
					$sntlSdParams = array('stationTriplets' => $sntlTrip, 'elementCd' => 'SNWD', 'ordinal' => '1',
						'duration' => 'DAILY', 'beginDate' => $qYear.'-'.$qMonth.'-01', 'endDate' => $qYear.'-'.$qMonth.'-01', 'getFlags' => '0');
					$sntlSdResp = $soapclient->getData($sntlSdParams);
					//print "sntlSdResp\n";
					//print_r($sntlSdResp);
					if (is_soap_fault($sntlSdResp)){
						print "Got an error connecting to server.  Retrying...";
						sleep(1);
						$sntlSdResp = $soapclient->getData($sntlSdParams);
						if (is_soap_fault($sntlSdResp)){
							print "Two errors in a row for sntlSdResp.  Exiting.";
							exit;
						}
					}
					if (!isset($sntlSdResp->return->values)){
						//print "...and NO SD data.\n";
						continue;
					}else{
						//print "...but we do have SD data:\n";
						//print_r($sndResp);
						if ($sntlSdResp->return->values == 0){
							//print "\tand it is zero so we set SWE to zero.\n\n\n";
							$sdDate = $qYear.'-'.$qMonth.'-01';
							$sdData = 0.0;
							$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SDIRV ".$sdData."\n";
						}else{
							$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SDIRV ".$sdData."\n";
							continue;
						}
						//print "\n\n\n";
					}
				}else{
					//print "...and here is the data:\n";
					//print_r($sntResp);
					$sweDate = $qYear.'-'.$qMonth.'-01';
					$sweData = $sntlSweResp->return->values;
					$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SWIRV ".$sweData."\n";
					//print "$snDate - $snData\n";
					//print "\n\n\n";
				}
				
			}else{
				//print "No SnowTel around here...\n\n\n";
				continue;
			}
		}else{
			continue;
		}
		
		//build a point object for geoJSON file
		$pnt = new stdClass();
		$pnt->type = "Feature";
		$pnt->geometry = new stdClass();
		$pnt->geometry->type = "Point";
		$pnt->geometry->coordinates = array($lon,$lat);
		$pnt->metadata = new stdClass();
		$pnt->metadata->agency = "NRCS";
		$pnt->metadata->created = $unixDateNow;
		$pnt->properties = new stdClass();
		$pnt->properties->name = $name;
		$pnt->properties->lid = $nwsid;
		$pnt->properties->elev = $elev;
		$pnt->properties->dataname = "Snow Water Equivalent";
		$pnt->properties->datatype = "sw";
		$pnt->properties->data = array( date_format(new DateTime($sweDate),'U') * 1000, floatval($sweData));
		$pnt->properties->normdata = array( date_format(new DateTime($sweDate),'U') * 1000, floatval($sweNormData));
		
		//print "for ".$station ." ".$nwsid." data is: ".$sweData." Norm is: ".$sweNormData."\n";
		
		//if we have norm data compute percentage and store
		if ($sweNormData >0 ){
			$pctNorm = round(($sweData / $sweNormData)*100);
			$pnt->properties->pctnorm = array( date_format(new DateTime($sweDate),'U') * 1000, $pctNorm);
			$chpsShef[] = ".A ".$nwsid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SWIPV ".$pctNorm."\n";
		}else{
			$pnt->properties->pctnorm = array( date_format(new DateTime($sweDate),'U') * 1000, "NA");
		}
		//add feature to geoJSON
		$geoData->features[] = $pnt;
		
		//store list of lids with data
		if (!isset($lidList[$nwsid])){
			$lidList[] = $nwsid;
		}
	}
	
	//SD code for shef is SDIRM
	$missingLids = array_diff($stationList,$lidList);
	//print_r($missingLids);
	$chpsSWEShefMissing = array();
	foreach($missingLids as $lid){
		$chpsSWEShefMissing[] = ".A ".$lid." ".$year.$mon."01 Z DH00/DC".$year.$monthDay."0000/SWIRV -9.99\n";
	}
	
	
	
	//Get additional obs from env.gov.bc.ca
	//mss_report.csv is ALL CAPS so need to create an uppercase to title case map
	$keys=array_keys($stationList);
	$map=array();
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
		$parts = explode(",",$line);
		if (array_key_exists($parts[0],$map)){
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
		$chpsShef[] = ".A ".$nwsid." ".$cayear.$camon.$caday." Z DH00/DC".$cayear.$camon.$caday."0000/SDIRV ".$swe."\n";
		if ($norm_mm != ''){
			$norm = round($norm_mm * 0.0393701,1);
			$chpsShef[] = ".A ".$lid." ".$cayear.$camon.$caday." Z DH00/DC".$cayear.$camon.$caday."0000/SWIPV ".$precentNorm."\n";
		}
	}
	
	
	file_put_contents("NRCS2shef_chps_missing_".$mon.$yr.".txt",$chpsSWEShefMissing);

	file_put_contents("nrcs2shef_chps_".$mon.$yr.".txt", $chpsShef);

	exec('/usr/bin/scp /usr/local/apps/scripts/nrcs2shef/nrcs2shef_chps_*.txt ldad@ls1-acr:/data/Incoming');
?>
