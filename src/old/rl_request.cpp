/*-*- c++ -*-
 *
 * rl_request.cpp
 * author : KDr2
 *
 */

#include <iostream>
#include <sstream>
#include <algorithm>
#include <functional>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/socket.h>

#include <gmp.h>

#include "rl.h"
#include "rl_util.h"
#include "rl_server.h"
#include "rl_connection.h"
#include "rl_request.h"
#include "rl_compdata.h"

std::map<std::string,Request::COMMAND> Request::cmd_map;

void Request::init_cmd_map()
{
    /* sys commands */
    Request::cmd_map["info"]       = &Request::rl_info;
    Request::cmd_map["keys"]       = &Request::rl_keys;
    Request::cmd_map["select"]     = &Request::rl_select;
    Request::cmd_map["multi"]      = &Request::rl_multi;
    Request::cmd_map["exec"]       = &Request::rl_exec;
    Request::cmd_map["discard"]    = &Request::rl_discard;

    /* kv commands */
    Request::cmd_map["incr"]       = &Request::rl_incr;
    Request::cmd_map["incrby"]     = &Request::rl_incrby;
    Request::cmd_map["get"]        = &Request::rl_get;
    Request::cmd_map["set"]        = &Request::rl_set;
    Request::cmd_map["del"]        = &Request::rl_del;
    Request::cmd_map["mget"]       = &Request::rl_mget;
    Request::cmd_map["mset"]       = &Request::rl_mset;

    /* set commands */
    Request::cmd_map["sadd"]       = &Request::rl_sadd;
    Request::cmd_map["srem"]       = &Request::rl_srem;
    Request::cmd_map["scard"]      = &Request::rl_scard;
    Request::cmd_map["smembers"]   = &Request::rl_smembers;
    Request::cmd_map["sismember"]  = &Request::rl_sismember;

    /* hash commands */
    Request::cmd_map["hget"]       = &Request::rl_hget;
    Request::cmd_map["hset"]       = &Request::rl_hset;
    Request::cmd_map["hsetnx"]     = &Request::rl_hsetnx;
    Request::cmd_map["hdel"]       = &Request::rl_hdel;
    Request::cmd_map["hexists"]    = &Request::rl_hexists;
    Request::cmd_map["hgetall"]    = &Request::rl_hgetall;
    Request::cmd_map["hkeys"]      = &Request::rl_hkeys;
    Request::cmd_map["hvals"]      = &Request::rl_hvals;
    Request::cmd_map["hlen"]       = &Request::rl_hlen;
}


Request::Request(Connection *c):
    connection(c), arg_count(-1), name("")
{
}

Request::~Request()
{
    std::vector<Request*>::iterator it=subrequest.begin();
    for(;it!=subrequest.end();it++)
        delete (*it);
}


void Request::append_arg(std::string arg)
{
    args.push_back(arg);
}

void Request::_run()
{

#ifdef DEBUG
    printf("Request Name:%s\n",name.c_str());
    for(std::vector<std::string>::iterator it=args.begin();it!=args.end();it++)
        printf("Request arg:%s\n",it->c_str());
#endif

    std::map<std::string,COMMAND>::iterator it=cmd_map.find(name);
    if(it!=cmd_map.end()){
        (this->*(it->second))();
    }else{
        rl_dummy();
    }

}

void Request::run()
{
    if(name=="multi"){
        _run();
        return;
    }
    if(name=="exec"||name=="discard"){

#ifdef DEBUG
        printf("Subrequest Number in this Transaction:%ld\n",
               connection->transaction->subrequest.size());
#endif

        if(connection->transaction){
            _run();
        }else{
            connection->write_error("ERR EXEC/DISCARD without MULTI");
        }
        return;
    }

    if(connection->transaction){
        connection->transaction->subrequest.push_back(this);
        connection->current_request=NULL;
        connection->write_status("QUEUED");
    }else{
        _run();
    }
}

void Request::rl_dummy(){
    connection->write_error("ERR unknown command");
}


void Request::rl_select(){
    if(connection->server->db_num<1){
        connection->write_error("ERR redis-leveldb is running in single-db mode");
        return;
    }
    if(args.size()!=1){
        connection->write_error("ERR wrong number of arguments for 'select' command");
        return;
    }
    if(std::find_if(args[0].begin(),args[0].end(),
                    std::not1(std::ptr_fun(isdigit)))!=args[0].end()){
        connection->write_error("ERR argument for 'select' must be a number");
        return;
    }
    int target=strtol(args[0].c_str(),NULL,10);
    if(target<0||target>=connection->server->db_num){
        connection->write_error("ERR invalid DB index");
        return;
    }
    connection->db_index=target;
    connection->write_status("OK");
}

void Request::rl_multi(){
    if(connection->transaction){
        connection->write_error("ERR MULTI calls can not be nested");
        return;
    }
    connection->transaction=this;
    connection->current_request=NULL;
    connection->write_status("OK");
}

void Request::rl_exec(){
    if(!connection->transaction){
        connection->write_error("ERR EXEC without MULTI");
        return;
    }

    std::vector<Request*> tsub=connection->transaction->subrequest;

    connection->write_mbulk_header(tsub.size());
    std::for_each(tsub.begin(),tsub.end(),std::mem_fun(&Request::_run));
    delete connection->transaction;
    connection->transaction=NULL;
}

void Request::rl_discard(){
    if(!connection->transaction){
        connection->write_error("ERR DISCARD without MULTI");
        return;
    }

    delete connection->transaction;
    connection->transaction=NULL;
    connection->write_status("OK");
}


void Request::rl_keys(){

    if(args.size()!=1){
        connection->write_error("ERR wrong number of arguments for 'keys' command");
        return;
    }

    std::vector<std::string> keys;
    size_t arg_len=args[0].size();
    bool allkeys = (arg_len==1 && args[0][0]=='*');
    if(arg_len>0){
        leveldb_iterator_t *kit = leveldb_create_iterator(connection->server->db[connection->db_index],
                                                          connection->server->read_options);
        const char *key;
        size_t key_len;
        leveldb_iter_seek_to_first(kit);
        while(leveldb_iter_valid(kit)) {
            key = leveldb_iter_key(kit, &key_len);
            if(allkeys || stringmatchlen(args[0].c_str(), arg_len, key, key_len, 0)){
                keys.push_back(std::string(key,key_len));
            }
            leveldb_iter_next(kit);
        }
        leveldb_iter_destroy(kit);
    }else{
        keys.push_back("");
    }

    connection->write_mbulk_header(keys.size());
    //std::for_each(keys.begin(), keys.end(), std::bind1st(std::mem_fun(&Connection::write_bulk),connection));
    std::vector<std::string>::iterator it=keys.begin();
    while(it!=keys.end())connection->write_bulk(*it++);
}



void Request::rl_info(){

    if(args.size()>1){
        connection->write_error("ERR wrong number of arguments for 'info' command");
        return;
    }

    std::ostringstream info;
    char *out=NULL;

    info << "redis_version: redis-leveldb " VERSION_STR "\r\n";
    info << "mode: ";
    if(connection->server->db_num<1){
        info << "single\r\n";
    }else{
        info << "multi = " << connection->server->db_num;
        info << "," << connection->db_index << "\r\n";
    }
    char *cwd=getcwd(NULL,0);
    info << "work_dir: " << cwd << "\r\n";
    free(cwd);
    info << "data_path: " << connection->server->db_path << "\r\n";
    info << "clients_num: " << connection->server->clients_num << "\r\n";

    out=leveldb_property_value(connection->server->db[connection->db_index],"leveldb.stats");
    if(out){
        info<< "stats: " << out <<"\r\n";
        free(out);
    }

    /* kyes num */
    if(args.size()>0 && args[0].find('k')!=std::string::npos){
        uint64_t key_num=0;
        leveldb_iterator_t *kit = leveldb_create_iterator(connection->server->db[connection->db_index],
                                                          connection->server->read_options);

        leveldb_iter_seek_to_first(kit);
        while(leveldb_iter_valid(kit)){
            key_num++;
            leveldb_iter_next(kit);
        }
        leveldb_iter_destroy(kit);
        info<< "keys: " << key_num <<"\r\n";
    }

    /** sstables info */
    if(args.size()>0 && args[0].find('t')!=std::string::npos){
        out=leveldb_property_value(connection->server->db[connection->db_index],"leveldb.sstables");
        if(out){
            info<< "sstables:\r\n" << out <<"\r\n";
            free(out);
        }
    }
    connection->write_bulk(info.str());
}
