/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * nic model: provisioning functions for nics and IPs
 */

'use strict';

var assert = require('assert-plus');
var common = require('./common');
var constants = require('../../util/constants');
var errors = require('../../util/errors');
var jsprim = require('jsprim');
var mod_ip = require('../ip');
var mod_net = require('../network');
var Nic = require('./obj').Nic;
var restify = require('restify');
var util = require('util');
var util_common = require('../../util/common');
var util_mac = require('../../util/mac');
var vasync = require('vasync');
var VError = require('verror');



// --- Internal functions


/**
 * If we have an existing NIC and it has provisioned IP addresses,
 * check if it contains any addresses that we're no longer using,
 * and free them.
 */
function freeOldIPs(opts, callback) {
    if (opts._removeIPs) {
        opts._removeIPs.forEach(function (oldIP) {
            opts.batch.push(oldIP.batch({ free: true }));
        });
    }

    callback();
}


function Provisioner() { }

/**
 * Grab the next available IP address on the currently selected network.
 */
Provisioner.prototype.fetchNextIP =
    function fetchNextIP(opts, dontStop, callback) {
    var self = this;
    assert.object(self.network, 'Network selected');
    mod_ip.nextIPonNetwork(opts, self.network, function (err, ip) {
        if (err) {
            if (dontStop && err.stop) {
                delete err.stop;
            }
            callback(err);
            return;
        }

        self.ip = ip;
        self.batchIP(opts, callback);
    });
};


/**
 * Push the selected IP address and its batched form into nicAndIP's arrays.
 */
Provisioner.prototype.batchIP = function batchCurIP(opts, callback) {
    assert.ok(this.ip, 'IP selected');
    opts.ips.push(this.ip);
    opts.batch.push(this.ip.batch());
    callback();
};


/**
 * If there was a previous error, check if it was because of our chosen IP.
 */
Provisioner.prototype.haveEtagFailure = function checkEtagFail(err) {
    if (!err) {
        // No error yet.
        return false;
    }

    if (this.ip === null) {
        // We haven't selected an IP yet.
        return false;
    }

    var cause = VError.findCauseByName(err, 'EtagConflictError');
    if (!cause) {
        return false;
    }

    var key = this.ip.key();
    var bucket = mod_ip.bucketName(this.network.uuid);
    return (cause.context.bucket === bucket && cause.context.key === key);
};


/**
 * Provisioner for handling specifically requested IP addresses.
 */
function IPProvision(ip, field) {
    this.ip = ip;
    this.network = ip.params.network.uuid;
    this.field = field;

    Object.seal(this);
}
util.inherits(IPProvision, Provisioner);


IPProvision.prototype.provision = function provisionIP(opts, callback) {
    if (this.haveEtagFailure(opts.err)) {
        var usedIP = this.ip.address.toString();
        var usedNet = this.network.uuid;
        var usedMsg = util.format(constants.fmt.IP_EXISTS, usedIP, usedNet);
        var usedErr = new errors.InvalidParamsError(
            constants.msg.INVALID_PARAMS,
            [ errors.duplicateParam(this.field, usedMsg) ]);
        usedErr.stop = true;
        callback(usedErr);
    } else {
        this.batchIP(opts, callback);
    }
};


/**
 * Provisioner for finding available IPs on requested networks.
 */
function NetworkProvision(network) {
    assert.object(network, 'network');

    this.ip = null;
    this.network = network;

    Object.seal(this);
}
util.inherits(NetworkProvision, Provisioner);


NetworkProvision.prototype.provision = function provisionNet(opts, callback) {
    if (this.ip === null || this.haveEtagFailure(opts.err)) {
        // We haven't chosen an IP yet, or the previous one was taken
        // by someone else.
        this.fetchNextIP(opts, false, callback);
    } else {
        // Reuse the already selected IP.
        this.batchIP(opts, callback);
    }
};


/**
 * Provisioner for finding IPs on networks in a given pool.
 */
function NetworkPoolProvision(pool, field) {
    assert.object(pool, 'pool');
    assert.string(field, 'field');

    this.ip = null;
    this.network = null;
    this.pool = pool;
    this.field = field;
    this.poolUUIDs = pool.networks;

    Object.seal(this);
}
util.inherits(NetworkPoolProvision, Provisioner);


/**
 * Move on to the next network pool, and provision an IP from it.
 */
NetworkPoolProvision.prototype.nextNetwork =
    function nextNetworkPool(opts, callback) {
    var self = this;

    var nextUUID = self.poolUUIDs.shift();
    if (!nextUUID) {
        var fullErr = new errors.InvalidParamsError('Invalid parameters',
            [ errors.invalidParam(self.field,
                constants.POOL_FULL_MSG) ]);
        fullErr.stop = true;
        callback(fullErr);
        return;
    }

    opts.log.debug({ nextUUID: nextUUID }, 'Trying next network in pool');

    var netGetOpts = {
        app: opts.app,
        log: opts.log,
        params: { uuid: nextUUID }
    };

    mod_net.get(netGetOpts, function (err, res) {
        if (err) {
            opts.log.error(err,
                'NetworkPoolProvision.nextNetwork(): error getting network %s',
                nextUUID);
            callback(err);
            return;
        }

        self.network = res;

        self.fetchNextIP(opts, true, callback);
    });
};


/**
 * Check if we've failed a provision on the currently selected network.
 */
NetworkPoolProvision.prototype.haveNetFailure = function (err) {
    if (!err) {
        // No error yet.
        return false;
    }


    if (err.name !== 'SubnetFullError') {
        return false;
    }

    return (err.network_uuid === this.network.uuid);
};


NetworkPoolProvision.prototype.provision =
    function provisionPool(opts, callback) {

    if (this.network === null || this.haveNetFailure(opts.err)) {
        // We haven't selected a network, or the chosen one is full.
        this.nextNetwork(opts, callback);
    } else if (this.ip === null || this.haveEtagFailure(opts.err)) {
        // Our selected IP has been taken: pick another
        this.fetchNextIP(opts, true, callback);
    } else {
        // Our current selection is fine, try it again
        this.batchIP(opts, callback);
    }
};


/**
 * Test if we've failed to provision a new NIC due to a conflict in MAC address.
 */
function nicEtagFail(err) {
    if (!err) {
        return false;
    }

    var cause = VError.findCauseByName(err, 'EtagConflictError');
    if (!cause) {
        return false;
    }

    return (cause.context.bucket === common.BUCKET.name);
}


/**
 * Adds an opts.nic with the MAC address from opts.validated, and adds its
 * batch item to opts.batch.  Intended to be passed to nicAndIP() in
 * opts.nicFn.
 */
function macSupplied(opts, callback) {
    // We've already tried provisioning once, and it was the nic that failed:
    // no sense in retrying

    opts.log.debug({}, 'macSupplied: enter');

    if (opts.nic && nicEtagFail(opts.err)) {
        var usedErr = new errors.InvalidParamsError(
            constants.msg.INVALID_PARAMS, [ errors.duplicateParam('mac') ]);
        usedErr.stop = true;
        callback(usedErr);
        return;
    }

    opts.nic = new Nic(opts.validated);
    if (opts.ips.length > 0) {
        assert.equal(opts.ips.length, 1, 'opts.ips.length === 1');
        opts.nic.ip = opts.ips[0];
        opts.nic.network = opts.nic.ip.params.network;
    }

    if (opts.nic.isFabric() && opts.vnetCns) {
        opts.nic.vnetCns = opts.vnetCns;
    }

    callback();
}


/**
 * Adds an opts.nic with a random MAC address, and adds its batch item to
 * opts.batch.  Intended to be passed to nicAndIP() in opts.nicFn.
 */
function randomMAC(opts, callback) {
    var validated = opts.validated;

    if (!opts.hasOwnProperty('macTries')) {
        opts.macTries = 0;
    }

    opts.log.debug({ tries: opts.macTries }, 'randomMAC: entry');

    // If we've already supplied a MAC address and the error isn't for our
    // bucket, we don't need to generate a new MAC - just re-add the existing
    // nic to the batch
    if (validated.mac && !nicEtagFail(opts.err)) {
        opts.nic = new Nic(validated);
        if (opts.ips.length > 0) {
            assert.equal(opts.ips.length, 1, 'opts.ips.length === 1');
            opts.nic.ip = opts.ips[0];
            opts.nic.network = opts.nic.ip.params.network;
        }

        callback();
        return;
    }

    if (opts.macTries > constants.MAC_RETRIES) {
        opts.log.error({
            start: opts.startMac,
            num: validated.mac,
            tries: opts.macTries
        }, 'Could not provision nic after %d tries', opts.macTries);
        var err = new restify.InternalError('no more free MAC addresses');
        err.stop = true;
        callback(err);
        return;
    }

    opts.macTries++;

    if (!opts.maxMac) {
        opts.maxMac = util_mac.maxOUInum(opts.app.config.macOUI);
    }

    if (!validated.mac) {
        validated.mac = util_mac.randomNum(opts.app.config.macOUI);
        opts.startMac = validated.mac;
    } else {
        validated.mac++;
    }

    if (validated.mac > opts.maxMac) {
        // We've gone over the maximum MAC number - start from a different
        // random number
        validated.mac = util_mac.randomNum(opts.app.config.macOUI);
    }

    opts.nic = new Nic(validated);
    if (opts.ips.length > 0) {
        assert.equal(opts.ips.length, 1, 'opts.ips.length === 1');
        opts.nic.ip = opts.ips[0];
        opts.nic.network = opts.nic.ip.params.network;
    }

    opts.log.debug({}, 'randomMAC: exit');
    callback();
}



// --- Exported functions



/**
 * Adds parameters to opts for provisioning a nic and an optional IP
 */
function addParams(opts, callback) {
    opts.nicFn = opts.validated.mac ? macSupplied : randomMAC;
    opts.baseParams = mod_ip.params(opts.validated);
    if (opts.validated.hasOwnProperty('_ip')) {
        opts._provisionableIPs = [ opts.validated._ip ];
    }
    return callback();
}

/**
 * Add the batch item for the nic in opts.nic opts.batch, as well as an
 * item for unsetting other primaries owned by the same owner, if required.
 */
function addNicToBatch(opts) {
    opts.log.debug({
        vnetCns: opts.vnetCns,
        ip: opts.nic.ip ? opts.nic.ip.v6address : 'none'
    }, 'addNicToBatch: entry');
    opts.batch = opts.batch.concat(opts.nic.batch({
       log: opts.log,
       vnetCns: opts.vnetCns
    }));
}


/**
 * If the network provided is a fabric network, fetch the list of CNs also
 * on that fabric network, for the purpose of SVP log generation.
 */
function listVnetCns(opts, callback) {
    // Collect networks that the NIC's on.
    assert.array(opts.ips, 'ips');
    var networks = {};

    opts.ips.forEach(function (ip) {
        var network = ip.params.network;
        if (network.fabric) {
            networks[network.uuid] = network;
        }
    });

    // We aren't on any fabric networks.
    if (jsprim.isEmpty(networks)) {
        callback(null);
        return;
    }

    vasync.forEachParallel({
        'inputs': Object.keys(networks),
        'func': function (uuid, cb) {
            var listOpts = {
                moray: opts.app.moray,
                log: opts.log,
                vnet_id: networks[uuid].vnet_id
            };

            common.listVnetCns(listOpts, cb);
        }
    }, function (err, res) {
        if (err) {
            callback(err);
            return;
        }

        opts.vnetCns = res.operations.reduce(function (acc, curr) {
            return acc.concat(curr.result);
        }, []);

        opts.log.debug({ vnetCns: opts.vnetCns }, 'provision.listVnetCns exit');

        callback(null);
    });
}


function nicBatch(opts, cb) {
    opts.log.debug({ vnetCns: opts.vnetCns }, 'nicBatch: entry');
    addNicToBatch(opts);

    opts.log.debug({ batch: opts.batch }, 'nicBatch: exit');
    return cb();
}


function runProvisions(opts, provisioners, callback) {
    vasync.forEachPipeline({
        inputs: provisioners,
        func: function (provisioner, cb) {
            provisioner.provision(opts, cb);
        }
    }, callback);
}


/**
 * Provisions a NIC and optional IPs. This code uses Moray etags on each object
 * it creates/updates inside its .batch() to avoid conflicting with concurrent
 * requests. If a conflict occurs, the provision attempt is restarted, and new
 * IPs or MAC addresses selected as needed.
 *
 * @param opts {Object}:
 * - baseParams {Object}: parameters used for creating the IP
 * - nicFn {Function}: function that populates opts.nic
 */
function nicAndIP(opts, callback) {
    assert.object(opts.baseParams, 'opts.baseParams');
    assert.func(opts.nicFn, 'opts.nicFn');

    var params = opts.validated;

    var provisioners = [];

    if (params._ip) {
        // Want a specific IP
        assert.array(opts._provisionableIPs, 'provisionableIPs');
        opts._provisionableIPs.forEach(function (ip) {
            var updated = mod_ip.createUpdated(ip, opts.baseParams);
            provisioners.push(new IPProvision(updated));
        });
    } else if (params.network_pool) {
        provisioners.push(
            new NetworkPoolProvision(params.network_pool, 'network_uuid'));
    } else if (params.network) {
        // Just provision the next IP on the network
        provisioners.push(
            new NetworkProvision(params.network, 'network_uuid'));
    }

    opts.log.debug({
        nicProvFn: opts.nicFn.name,
        // We could only be provisioning a nic:
        ipProvFn: provisioners.length === 0 ? 'none' : 'some',
        baseParams: opts.baseParams,
        validated: opts.validated,
        vnetCns: opts.vnetCns || 'none'
    }, 'provisioning nicAndIP');

    util_common.repeat(function (cb) {
        // Reset opts.{batch,ips} - it is the responsibility for functions in
        // the pipeline to re-add their batch data each time through the loop.
        opts.batch = [];
        opts.ips = [];

        vasync.pipeline({
            arg: opts,
            funcs: [
                // 1. Determine what IPs to provision and batch them.
                function (_, cb2) { runProvisions(opts, provisioners, cb2); },

                // 2. Free any addresses we no longer need.
                freeOldIPs,

                // 3. Locate the CNs we need to inform of overlay IP changes.
                listVnetCns,

                // 4. Using our IPs, create the NIC object.
                opts.nicFn,

                // 5. Batch the NIC.
                nicBatch,

                // 6. Commit everything in our batch.
                common.commitBatch
            ]
        }, function (err) {
            if (err) {
                opts.log.warn({ err: err, final: err.stop }, 'error in repeat');
                if (err.stop) {
                    // No more to be done:
                    cb(err, null, false);
                    return;
                }

                // Need to retry. Set opts.err so the functions in funcs
                // can determine if they need to change their params.
                opts.err = err;
                cb(null, null, true);
                return;
            }

            cb(null, opts.nic, false);
        });
    }, function (err, res) {
        if (err) {
            callback(err);
            return;
        }

        opts.log.info({ params: params, obj: res.serialize() }, 'Created nic');

        callback(null, res);
    });
}

module.exports = {
    addParams: addParams,
    addNicToBatch: addNicToBatch,
    nicAndIP: nicAndIP
};
