var Promise = require("bluebird");
var retry = require('retry');
var _ = require('lodash');

var google = require('googleapis');
var googleAuth = require('google-auth-library');
var bigquery = Promise.promisifyAll(google.bigquery('v2').jobs);

var aws_config = require("./aws_credentials.json");
var gcp_config = require("./gcp_credentials.json");

var storagetransfer_transferJobs=Promise.promisifyAll(google.storagetransfer('v1').transferJobs);
var storagetransfer_transferOperations=Promise.promisifyAll(google.storagetransfer('v1').transferOperations);


var s3_backet_name = "almalogic-redshift-export";
var gcs_bucket = "ssb-s3-transfer";
var gcs_project = "juvogrid";
var gcp_credential_file = "gcp_credentials.json";

var aws_accessKeyId =  aws_config.aws_accessKeyId;
var aws_secretAccessKey =  aws_config.aws_secretAccessKey;

var authClient;

var createGCPTransferRequest = function(object){
  var resolver = Promise.defer();

  var fileKey = object.s3.object.key;
  var prefixes = [];
  prefixes.push(fileKey);

  var objectConditions = {
    "includePrefixes": prefixes,
  }

  var TransferSpec = {
    "objectConditions": objectConditions,
    "transferOptions": {
      "overwriteObjectsAlreadyExistingInSink": true,
      "deleteObjectsUniqueInSink": false,
      "deleteObjectsFromSourceAfterTransfer": false,
    },
    "awsS3DataSource": {
      "bucketName": object.s3.bucket.name,
      "awsAccessKey":
      {
        "accessKeyId": aws_accessKeyId,
        "secretAccessKey": aws_secretAccessKey,
      },
    },
    "gcsDataSink": {
      "bucketName": gcs_bucket,
    }
  };

  var date = new Date();

  var job_start_day = {
    "year": date.getFullYear(),
    "month": date.getMonth() + 1,
    "day": date.getDate(),
  };

  var Schedule = {
    "scheduleStartDate": job_start_day,
    "scheduleEndDate": job_start_day,
  };

  var transfer_resource = {
    "description": fileKey+" transfer",
    "projectId": gcs_project,
    "transferSpec": TransferSpec,
    "schedule": Schedule,
    "status": "ENABLED",
  };

  var request = {
    resource: transfer_resource,
    auth: authClient
  };

  return storagetransfer_transferJobs.createAsync(request);
}

var getGCPTransferStatus = function(job) {
  var resolver = Promise.defer();

  var job_name = job.name;
  var operation = retry.operation({
    retries: 5,
    factor: 3,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: true,
  });

  var request = {
    filter:'{"project_id":"'+gcs_project+'","job_names":["'+job_name+'"]}',
    name: "transferOperations",
    auth: authClient
  };

  operation.attempt(function(currentAttempt) {
    storagetransfer_transferOperations.list(request, function(err, result) {
      if (result.operations && result.operations[0].done){
        resolver.resolve(result);
        operation.stop();
      } else {
        err = "Not completed yet";
      }
      if (operation.retry(err)) {
        return;
      }

    });
  });

  return resolver.promise;
}

var getAuth = function(){
  var resolver = Promise.defer();

  if(authClient) {
    return Promise.resolve(authClient);
  }

  //console.log(credentials);
  var auth = new googleAuth();
  var oauth2Client = new auth.OAuth2();
  var jwt = new google.auth.JWT(gcp_config.client_email,null,gcp_config.private_key,['https://www.googleapis.com/auth/cloud-platform']);
  jwt.authorize(function(err, result) {
    //console.log("Token",result.access_token);
    oauth2Client.setCredentials({
      access_token: result.access_token
    });
    authClient = oauth2Client;
    resolver.resolve(authClient);
  });

  return resolver.promise;
}

var createLoadJob = function(uris,table){

  var job = {
    configuration: {
      load: {
        sourceUris: 	uris,
        writeDisposition: "WRITE_APPEND",
        sourceFormat: "CSV",
        ignoreUnknownValues: true,
        fieldDelimiter: "|",
        createDisposition: "CREATE_NEVER",
        allowJaggedRows: true,
        destinationTable: {
          projectId: gcs_project,
          datasetId: "ssb",
          tableId: table
        },
        skipLeadingRows: 0
      }
    }
  };

  var request = {
    projectId: gcs_project,
    resource: job,
    auth: authClient
  };
  return bigquery.insertAsync(request);
}

var getLoadJobStatus= function(jobId){
  var resolver = Promise.defer();

  var operation = retry.operation({
    retries: 5,
    factor: 3,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: true,
  });

  var request = {
    projectId: gcs_project,
    jobId: jobId,
    auth: authClient
  };

  operation.attempt(function(currentAttempt) {
    bigquery.get(request, function(err, result) {
      //console.log(operation.attempts(), " - ", result);
      if (result.status && result.status.state==="DONE"){
        resolver.resolve(result);
        operation.stop();
      } else {
        err = "Not completed yet";
      }
      if (operation.retry(err)) {
        return;
      }
    });
  });

  return resolver.promise;
};

var pipeline = function(object){
  console.log("Starting New RedShift dump file processing");

  //We can add more steps here. Say if we need to run some SQL processing loaded data
  return createGCPTransferRequest(object)
  .then(function(result){
    return getGCPTransferStatus(result);
  })
  .then(function(result){
    console.log("New File Transfered to GCP");
    //need to put logic mapping object uploaded into table
    var prefixes = result.operations[0].metadata.transferSpec.objectConditions.includePrefixes;
    var uris = _.map(prefixes,function(prefix){
      return "gs://ssb-s3-transfer/"+prefix;
    });
    //console.log(prefixes,uris);
    var table_name = prefixes[0].split("_")[0];
    console.log("Staring BigQuery Logging uri into "+table_name+" ",uris);

    return createLoadJob(uris,table_name);
  })
  .then(function(result){
    return getLoadJobStatus(result.jobReference.jobId)
  });
}

exports.handler = (event, context, callback) => {
   // TODO implement
   console.log("New File Event Received");

   /*var event = {
    Records: [{
       s3:{
         object:{
           key:"part_0003_part_00"
         },
         bucket:{
           name:"almalogic-redshift-export"
         }
       }
     }]
   };*/

   event.Records.forEach(function(record){

     getAuth()
     .then(function(){
       console.log("Initiating Pipeline");
       return pipeline(record);
     })
     .then(function(result){
       console.log("New Data Loaded",result.statistics);
       callback(null,result.statistics);
     })
     .catch(function(err){
       console.log(err);
       callback(err,null);
     });

   });
};
