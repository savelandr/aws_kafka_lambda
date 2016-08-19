java_package 'i.hate.java'
require 'i/hate/java/zookeeper_utils'
require 'json'

class OffsetSkew

  java_signature "void handler(String config, com.amazonaws.services.lambda.runtime.Context context)"
  def handler(config, context)
    @config = JSON.parse config
    @logger = context.get_logger
    mins_early = @config["lookbackMinutes"] || 60
    @earliest = (Time.now.to_i - (mins_early.to_i * 60)) * 1000
    @latest = -1
    @logger.log "Starting with zookeeper: #{@config['zookeeperUrl']}"
    @logger.log "Starting with lookback: #{mins_early} minutes"
    @logger.log "Starting with topics: #{@config['topics'].inspect}"
    ZookeeperUtils.connect @config['zookeeperUrl']
    topics = @config['topics']
    topics.each {|topic| partition_skew topic}
    ZookeeperUtils.close
  end

  def partition_skew(topic)
    brokers = ZookeeperUtils.brokers
    partitions = ZookeeperUtils.partitions topic
    results = []
    partitions.each do |partition|
      broker = brokers.find {|b| b[:id].to_i == partition[:leader]}
      @logger.log "No broker found for partition: #{partition[:partition]}" unless broker
      delta = offset_delta(broker, partition[:partition].to_i, topic) if broker
      results << delta
    end
    min = results.compact.min
    max = results.compact.max
    skew = (max.to_f - min.to_f) / min.to_f
    skew = 0.0 if skew.send("nan?") #stupid java and ? methods
    skew = 100000.0 if skew.send("infinite?")
    @logger.log "Topic: #{topic} has skew of: #{skew * 100} percent\n"
  end

  def offset_delta(broker, partition, topic)
    consumer = Java::KafkaConsumer::SimpleConsumer.new(broker[:host], broker[:port],100000, 64 * 1024, "lambda-skew-checker")
    begin
      t_and_p = Java::KafkaCommon::TopicAndPartition.new(topic, partition)
      early_part = Java::KafkaApi::PartitionOffsetRequestInfo.new(@earliest, 1)
      late_part = Java::KafkaApi::PartitionOffsetRequestInfo.new(@latest, 1)
      early_map = Java::ScalaCollectionImmutable::Map::Map1.new t_and_p, early_part
      late_map = Java::ScalaCollectionImmutable::Map::Map1.new t_and_p, late_part
      early_resp = consumer.get_offsets_before(Java::KafkaApi::OffsetRequest.new(early_map, 0, 0, "", -1))
      late_resp = consumer.get_offsets_before(Java::KafkaApi::OffsetRequest.new(late_map, 0, 0, "", -1))
      earliest_offset = early_resp.partition_error_and_offsets[t_and_p].offsets.last
      latest_offset = late_resp.partition_error_and_offsets[t_and_p].offsets.last
      delta = latest_offset - earliest_offset
      @logger.log "Topic: #{topic} partition: #{partition} has delta of: #{delta}\n"
      return delta
    rescue
      @logger.log "Unable to find offset for partition: #{partition} on broker: #{broker}\n"
      return nil
    ensure
     consumer.close
    end
  end
end
