java_package 'i.hate.java'
require 'i/hate/java/zookeeper_utils'
require 'json'

class OffsetSkew

  java_signature "void handler(String config, com.amazonaws.services.lambda.runtime.Context context)"
  def handler(config, context)
    config = JSON.parse config
    logger = context.get_logger
    ZookeeperUtils.connect config['zookeeperUrl']
    logger.log "Brokers: #{ZookeeperUtils.broker_url}"
  end
end
