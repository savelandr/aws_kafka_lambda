java_package 'i.hate.java'
class SomeWatcher
  include Java::OrgApacheZookeeper::Watcher

  def process(event)
    puts "Watcher: #{event.to_s}" if $DEBUG
  end
end
