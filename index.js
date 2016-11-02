var fs = require('fs'),
  backup = require('mongodb-backup'),
  targz = require('tar.gz'),
  AWS = require('aws-sdk'),
  MongoURI = require('mongo-uri')



var config = {};
config.mongoUrl = process.env.MONGO_URL;
config.environment = process.env.ENVIRONMENT || 'staging';
config.accessKeyId = process.env.AWS_ACCESS_KEY;
config.secretAccessKey = process.env.AWS_SECRET_KEY
config.bucket = process.env.BUCKET || 'gf-mongodb-backup'
console.log(JSON.stringify(config, null, 2))

// Uncomment below for testing this project using AWS credentials
AWS.config.update({
  accessKeyId: config.accessKeyId,
  secretAccessKey: config.secretAccessKey
});

var consoleLog = function (obj, msg) {
  console.log(msg);
}

//exports.handler = function(event, context, callbackFunc) {
var callbackFunc = consoleLog;

var parsedUri;
try {
  parsedUri = MongoURI.parse(config.mongoUrl);
} catch (err) {
  // handle this correctly, kthxbye
}
var database = parsedUri.database;

// Create a filename for the backup file
var timestamp = new Date().toISOString().replace(/\..+/g, '').replace(/[-:]/g, '').replace(/T/g, '-');

var tempDir = '/tmp/mongodb-backups';
var filename = database + '-' + timestamp + '.tar.gz';

// Remove a given directory
var deleteFolderRecursive = function (path) {
  if (fs.existsSync(path)) {
    fs.readdirSync(path).forEach(function (file, index) {
      var curPath = path + "/" + file;
      if (fs.lstatSync(curPath).isDirectory()) { // recurse
        deleteFolderRecursive(curPath);
      } else { // delete file
        fs.unlinkSync(curPath);
      }
    });
    fs.rmdirSync(path);
  }
};

// Clean up the temporary directory used for holding backup data
var cleanUpTempDir = function () {
  fs.unlink(tempDir + '/' + filename, function (err) {
    if (err) {
      callbackFunc(err, "error in removing the backup .tar file");
    } else {
      deleteFolderRecursive(tempDir + '/' + database);
    }
  });
};

// Upload the backup file to an AWS S3 bucket
var uploadToAwsS3 = function (s3bucket) {
  var bucket = new AWS.S3({ params: { Bucket: s3bucket } });

  bucket.putObject({
    Key: config.environment + '/' + filename,
    Body: fs.createReadStream(tempDir + '/' + filename),
    ServerSideEncryption: 'AES256', // AES256 Server Side Encryption
  }, function (err, data) {
    // Clean up the temporary directory
    cleanUpTempDir();

    if (err) {
      callbackFunc(err, "error in uploading the backup file to S3 bucket");
    } else {
      callbackFunc(null, "Uploaded a backup file [" + filename
        + "] to a S3 bucket [" + s3bucket + "]");
    }
  });
};

// Connect to the database in MongoDB and back up .bson files for all collections
backup({
  uri: config.mongoUrl,
  root: tempDir,
  callback: function (err) {
    if (err) {
      callbackFunc(err, "error in backing up MongoDB database");
    } else {
      targz().compress(tempDir + '/' + database, tempDir + '/' + filename)
        .then(function () {
          console.log('Compression done!');

          uploadToAwsS3(config.bucket);
        })
        .catch(function (err) {
          console.log('Something is wrong with compression ', err.stack);
        });
    }
  }
});
// }
