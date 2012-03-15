# encoding: utf-8

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new

namespace :spec do
  RSpec::Core::RakeTask.new('buffer') do |t|
    t.pattern = 'spec/**/buffer_spec.rb'
  end
end

namespace :spec do
  RSpec::Core::RakeTask.new('datatype') do |t|
    t.pattern = 'spec/**/datatype_spec.rb'
  end
end

namespace :spec do
  RSpec::Core::RakeTask.new('message') do |t|
    t.pattern = 'spec/**/message_spec.rb'
  end
end
