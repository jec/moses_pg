# encoding: utf-8

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new

namespace :spec do
  RSpec::Core::RakeTask.new('buffer') do |t|
    t.pattern = 'spec/**/buffer_spec.rb'
  end
end
