<script runat="server">
Platform.Load("Core", "1.1.1");
  
Array.prototype.map = function(cb) {
  var oldArr = this;
  var newArr = [];

  for (var i = 0; i < oldArr.length; i++) {
    newArr.push(cb(oldArr[i], i, this));
  }

  return newArr;
}

Array.prototype.filter = function(cb) {
  var oldArr = this;
  var newArr = [];

  for (var i = 0; i < oldArr.length; i++) {
    if(cb(oldArr[i], i, this))
      newArr.push(oldArr[i]);
  }

  return newArr;
}

try {
  var api = new Script.Util.WSProxy();
  
  var cols = ['Name', 'CustomerKey'];
  var request = api.retrieve('DataExtension', cols);
  var results = request.Results;
  
  results = results.map(function(result) {
    return {
      name: result['Name'],
      external_key: result['CustomerKey']
    };
  });
  
  results = results.filter(function(result) {
    var notSystemDe = result.name[0] !== '_';
    var notDataSystemDe = result.name.indexOf('dts') === -1;
    var notIgoDe = result.name.indexOf('IGO_') === -1;
    var notEinsteinDe = result.name.indexOf('Einstein_MC_') === -1;
    var notPredictiveDe = result.name.indexOf('PI_') === -1;
    var notSocialPagesDe = result.name.indexOf('SocialPages_DataExtension') === -1;
    var notCloudPagesDe = result.name.indexOf('CloudPages_DataExtension') === -1;
    var notExpressionBuildeDe = result.name.indexOf('ExpressionBuilderAttributes') === -1;
    var notQueryStudio = result.name.indexOf('QueryStudioResults') === -1;
    var notMobileOrphan = result.name.indexOf('MobileLineOrphanContact') === -1;
    var notTestSend = result.name.indexOf('TestSendRecipient') === -1;
    var notSupportSimnulation = result.name.indexOf('SimulationSupportDE') === -1;
    
    return notSystemDe && notDataSystemDe && notIgoDe && notEinsteinDe && notPredictiveDe && notSocialPagesDe && notCloudPagesDe && notExpressionBuildeDe && notQueryStudio && notMobileOrphan && notTestSend && notSupportSimnulation;
  });
  
  HTTPHeader.SetValue('Access-Control-Allow-Methods', 'GET');
  HTTPHeader.SetValue('Content-Type', 'application/json');
  Write(Stringify(results));

} catch(error) {
 Write(Stringify(error));
} 
    
</script>