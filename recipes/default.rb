#
# Cookbook Name:: mongodb
# Recipe:: default
#
# Copyright 2011, edelight GmbH
# Authors:
#       Markus Korn <markus.korn@edelight.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include_recipe "aws_ebs_disk"

package "mongodb" do
  package_name "mongodb-10gen"
  version "2.4.3"
  action :install
end

needs_mongo_gem = (node.recipes.include?("mongodb::replicaset") or node.recipes.include?("mongodb::mongos"))

template "/etc/security/limits.conf" do
  mode "0644"
  owner "root"
  group "root"
  source "limits.conf.erb"
  variables(:limits => node[:mongodb][:ulimits])
end

if needs_mongo_gem
  # install the mongo ruby gem at compile time to make it globally available
  gem_package 'mongo' do
    version "1.10.2"
    action :nothing
  end.run_action(:install)
  Gem.clear_paths
end

if !node.recipes.include?("mongodb::configserver") and !node.recipes.include?("mongodb::shard") and !node.recipes.include?("mongodb::replicaset") and !node.recipes.include?("mongodb::mongos") and !node.recipes.include?("mongodb::mongos_app_server")
  # configure default instance
  mongodb_instance "mongodb" do
    mongodb_type "mongod"
    port         node['mongodb']['port']
    dbpath       node['mongodb']['dbpath']
    enable_rest  node['mongodb']['enable_rest']
  end
end

cookbook_file '/etc/sysctl.d/60-mongo.conf' do
   source 'mongo_sysctl.conf'
   owner 'root'
   mode '0644'
   action :create
end

bash "Update system variables" do
  code <<-BASH_SCRIPT
  user "root"
  service procps start
  BASH_SCRIPT
end

