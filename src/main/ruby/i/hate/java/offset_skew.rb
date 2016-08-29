java_package 'i.hate.java'
require 'i/hate/java/zookeeper_utils'
require 'json'

class OffsetSkew

  java_signature "void handler(String config, com.amazonaws.services.lambda.runtime.Context context)"
  def handler(config, context)
    @config = JSON.parse config
    @logger = Java::OrgApacheLog4j::Logger.get_logger(self.class.name)
    @mins_early = @config["lookbackMinutes"] || 60
    now = Time.now
    @earliest = (now.to_i - (@mins_early.to_i * 60)) * 1000
    @latest = (now.to_i) * 1000
    ZookeeperUtils.connect @config['zookeeperUrl']
    topics = @config['topics']
    topics.each {|topic| partition_skew topic}
    ZookeeperUtils.close
  end

  def partition_skew(topic)
    brokers = ZookeeperUtils.brokers
    partitions = ZookeeperUtils.partitions topic
    results = {}
    partitions.each do |partition|
      broker = brokers.find {|b| b[:id].to_i == partition[:leader]}
      @logger.error "No broker found for partition: #{partition[:partition]}" unless broker
      delta = offset_delta(broker, partition[:partition].to_i, topic) if broker
      results[partition[:partition]] = delta
    end
    results.sort_by(&:last).each {|k,v| @logger.info "Topic: #{topic} partition: #{k} has delta of: #{v}"}
    deltas = results.map {|k,v| v}.compact
    min = deltas.min
    max = deltas.max
    skew = (max.to_f - min.to_f) / min.to_f
    skew = 0.0 if skew.send("nan?") #stupid java and ? methods
    skew = 100000.0 if skew.send("infinite?")
    @logger.warn "Topic: #{topic} has skew of: #{"%.2f" % (skew * 100)}% over last #{@mins_early} minutes\n"
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
      return delta
    rescue
      @logger.error "Unable to find offset for partition: #{partition} on broker: #{broker}\n"
      return nil
    ensure
     consumer.close
    end
  end
end
