#
# Cookbook Name:: mongodb
# Definition:: mongodb
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

define :mongodb_instance, :mongodb_type => "mongod" , :action => [:enable, :start], :port => 27017 , \
    :logpath => nil, :dbpath => "/data/mongodb", :configfile => "/etc/mongodb.conf", \
    :configserver => [], :replicaset => nil, :enable_rest => false, :mongos_dependent_app => nil, \
    :notifies => [] do
    
  include_recipe "mongodb::default"
  
  name = params[:name]
  type = params[:mongodb_type]
  service_action = params[:action]
  service_notifies = params[:notifies]
  
  port = params[:port]

  dbpath = params[:dbpath]

  upstartfile = "/etc/init/#{name}.conf"
  rcfile = "/etc/init.d/#{name}"
  
  configfile = params[:configfile]
  configserver_nodes = params[:configserver]

  enable_rest = params[:enable_rest]
  
  replicaset = params[:replicaset]
  if type == "shard"
    if replicaset.nil?
      replicaset_name = nil
    else
      # for replicated shards we autogenerate the replicaset name for each shard
      replicaset_name = "rs_#{replicaset['mongodb']['shard_name']}"
    end
  else
    # if there is a predefined replicaset name we use it,
    # otherwise we try to generate one using 'rs_$SHARD_NAME'
    begin
      replicaset_name = replicaset['mongodb']['replicaset_name']
    rescue
      replicaset_name = nil
    end
    if replicaset_name.nil?
      begin
        replicaset_name = "rs_#{replicaset['mongodb']['shard_name']}"
      rescue
        replicaset_name = nil
      end
    end
  end
  
  if !["mongod", "shard", "configserver", "mongos"].include?(type)
    raise "Unknown mongodb type '#{type}'"
  end
  
  if type != "mongos"
    daemon = "/usr/bin/mongod"
    configserver = nil
  else
    daemon = "/usr/bin/mongos"
    dbpath = nil
 #   if node['fqdn'].nil? then
 #     configservers = configserver_nodes.collect{|n| "#{n['ipaddress']}:#{n['mongodb']['port']}" }.join(",")
 #   else
 #     configservers = configserver_nodes.collect{|n| "#{n['fqdn']}:#{n['mongodb']['port']}" }.join(",")
 #   end
    configservers = configserver_nodes.collect{|n| "#{n['ipaddress']}:#{n['mongodb']['port']}" }.join(",")
  end

  if type == "configserver"
    configsvr = true
  end
  
  if type == "shard"
    shardsvr = true
  end
  
  if type != "mongos"
    # dbpath dir [make sure it exists]
    directory dbpath do
      owner "mongodb"
      group "mongodb"
      mode "0755"
      action :create
      recursive true
    end
  end

  if type != "mongos"
     template_source = "mongodb.config.erb"
  else
     template_source = "mongos.config.erb"
  end
  
  # Setup DB Config File
  template "#{configfile}" do
    action :create
    source template_source
    group node['mongodb']['root_group']
    owner "root"
    mode 0644
    variables(
      "dbpath"		=> dbpath,
      "port"		=> port,
      "configdb"	=> configservers,
      "replicaset_name"	=> replicaset_name,
      "configsvr"	=> configsvr,
      "shardsvr"	=> shardsvr,
      "enable_rest"	=> enable_rest
    )
  end

  if node[:mongodb][:raid]
     mnt_point = node[:mongodb][:raid_mount]
     setra = node[:mongodb][:setra]
  else
     mnt_point = ''
     setra = ''
  end

  # Setup Upstart Config File
  if type != "mongos"
     template "#{upstartfile}" do
       action :create
       source "mongodb.upstart.erb"
       group node['mongodb']['root_group']
       owner "root"
       mode 0644
       variables(
	 "daemon" => daemon,
	 "configfile" => configfile,
	 "dbpath" => dbpath,
	 "mnt_point" => mnt_point,
	 "setra" => setra,
	 "type" => type,
	 "raid" => node[:mongodb][:raid]
       )
     end
   else
     template "#{upstartfile}" do
       action :create
       source "mongos.upstart.erb"
       group node['mongodb']['root_group']
       owner "root"
       mode 0644
       variables(
	 "daemon" => daemon,
	 "configfile" => configfile,
	 "mongos_dependent_app" => params[:mongos_dependent_app],
       )
     end
   end

  #
  # Install upstart-job link 
  #
  bash "Replacing MongoDB init.d script with upstart-job hook (#{rcfile})" do
    not_if { ::File.symlink?(rcfile) }
    if type != "mongos"
      code <<-BASH_SCRIPT
      rm -f /etc/init.d/mongodb
      ln -s /lib/init/upstart-job #{rcfile}
      BASH_SCRIPT
    else
      code <<-BASH_SCRIPT
      rm -f /etc/init.d/mongos
      ln -s /lib/init/upstart-job #{rcfile}
      BASH_SCRIPT
    end
  end

  # service
  service name do
    provider Chef::Provider::Service::Upstart
    sleep(60)
    supports :status => true, :start => true
    action service_action
    notifies :start, service_notifies
    if !replicaset_name.nil?
      notifies :create, "ruby_block[config_replicaset]", :delayed
    end
    if type == "mongos"
      notifies :create, "ruby_block[config_sharding]", :delayed
    end
  end
  
  # replicaset
  if !replicaset_name.nil?
    rs_nodes = search(
      :node,
      "mongodb_cluster_name:#{replicaset['mongodb']['cluster_name']} AND \
       recipes:mongodb\\:\\:replicaset AND \
       mongodb_shard_name:#{replicaset['mongodb']['shard_name']} AND \
       chef_environment:#{replicaset.chef_environment}"
    )
  
    ruby_block "config_replicaset" do
      block do
        if not replicaset.nil?
          MongoDB.configure_replicaset(replicaset, replicaset_name, rs_nodes)
        end
      end
      action :nothing
    end
  end
  
  # sharding
  if type == "mongos"
    # add all shards
    # configure the sharded collections
    
    shard_nodes = search(
      :node,
      "mongodb_cluster_name:#{node['mongodb']['cluster_name']} AND \
       recipes:mongodb\\:\\:shard AND \
       chef_environment:#{node.chef_environment}"
    )
    
    if not shard_nodes.nil?
       Chef::Log.info("Found #{shard_nodes.count()} shard nodes.")
       ruby_block "config_sharding" do
	 block do
	   if type == "mongos"
             Chef::Log.info("Waiting 30s for DB to start...")
             sleep(30)
             if not node['mongodb']['sharded_collections'].nil?
		Chef::Log.info("Adding shard nodes...")
		MongoDB.configure_shards(node, shard_nodes)
                Chef::Log.info("Sharding collections...")
	        MongoDB.configure_sharded_collections(node, node['mongodb']['sharded_collections'])
             else
                Chef::Log.info("No collections were sharded.")
             end
	   end
	 end
	 action :nothing
       end
    else
       Chef::Log.info("Found no shard nodes to configure.")
    end
  end

if %w{ ubuntu debian }.include? node.platform
    ruby_block "uncomment_pam_limits" do
      block do
        f = Chef::Util::FileEdit.new('/etc/pam.d/su')
        f.search_file_replace(/^\#\s+(session\s+required\s+pam_limits.so)/, '\1')
        f.write_file
        Chef::Log.info("Updating pam_limits file if necessary")
       end
      only_if "egrep '^#\s+session\s+required\s+pam_limits\.so\s*$' /etc/pam.d/su"
     end
   end
end
