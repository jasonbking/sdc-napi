/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * nic model: deleting
 */

'use strict';

var common = require('./common');
var getNic = require('./get').get;
var validate = require('../../util/validate');
var vasync = require('vasync');


// --- Internal

var DELETE_SCHEMA = {
    required: {
        mac: validate.MAC
    }
};

function validateDeleteParams(opts, callback) {
    validate.params(DELETE_SCHEMA, null, opts.params, function (err, res) {
        opts.validatedParams = res;
        return callback(err);
    });
}

function getExistingNic(opts, cb) {
    getNic(opts, function (err, nic) {
        opts.existingNic = nic;
        return cb(err);
    });
}

function listVnetCns(opts, cb) {
    if (!opts.existingNic.isFabric()) {
        return cb();
    }
    var listOpts = {
        vnet_id: opts.existingNic.network.vnet_id,
        moray: opts.app.moray,
        log: opts.log
    };
    common.listVnetCns(listOpts, function (listErr, vnetCns) {
        if (listErr) {
            return cb(listErr);
        }
        opts.vnetCns = vnetCns;
        return cb();
    });
}

function addNicToBatch(opts, cb) {
    opts.batch = opts.existingNic.delBatch({ log: opts.log,
        vnetCns: opts.vnetCns });
    return cb();
}

function delIPs(opts, callback) {
    if (!opts.existingNic || !opts.existingNic.ip) {
        opts.log.debug('nic: delete: nic "%s" has no IPs', opts.params.mac);
        callback();
        return;
    }

    vasync.forEachParallel({
        'inputs': [ opts.existingNic.ip ],
        'func': delIP.bind(null, opts)
    }, callback);
}


function delIP(opts, ip, cb) {
    if (ip.params.belongs_to_uuid === opts.existingNic.params.belongs_to_uuid) {
        opts.batch.push(ip.unassignBatch());
    } else {
        opts.log.warn({
            nic_owner: opts.existingNic.params.belongs_to_uuid,
            ip_owner: ip.params.belongs_to_uuid,
            mac: opts.params.mac,
            ip: ip.address
        }, 'nic: delete: IP and NIC belongs_to_uuid do not match');
    }

    cb();
}


// --- Exports



/**
 * Deletes a nic with the given parameters
 */
function del(opts, callback) {
    opts.log.debug({ params: opts.params }, 'nic: del: entry');

    vasync.pipeline({
        arg: opts,
        funcs: [
            validateDeleteParams,
            getExistingNic,
            listVnetCns,
            addNicToBatch,
            delIPs,
            common.commitBatch
        ]
    }, function (err) {
        if (err) {
            opts.log.error(err, 'nic: delete: error');
        }

        callback(err);
    });
}



module.exports = {
    del: del
};
