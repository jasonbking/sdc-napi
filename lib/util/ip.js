/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * IP-related utilities
 */

'use strict';

var ipaddr = require('ip6addr');
var net = require('net');


var MAX_IPV4 = 4294967295;


function invalidOctet(octet) {
    return isNaN(octet) || (octet < 0) || (octet > 255);
}


/*
 * Converts an array of four octets into a numeric representation
 * of an IPv4 address.
 */
function octetsToNumber(octets) {
    if (octets.length !== 4) {
        throw new Error(
            'Octets array doesn\'t have 4 items, but ' + octets.length);
    }

    var a = Number(octets[0]);
    var b = Number(octets[1]);
    var c = Number(octets[2]);
    var d = Number(octets[3]);

    if (invalidOctet(a) || invalidOctet(b) ||
        invalidOctet(c) || invalidOctet(d)) {
        throw new Error(
            'Octets array contains invalid octets: ' + JSON.stringify(octets));
    }

    return (a * 16777216) + (b * 65536) + (c * 256) + d;
}


/*
 * Converts a dotted IPv4 address (eg: 1.2.3.4) to its integer value
 */
function addressToNumber(addr) {
    if (!addr || !net.isIPv4(addr)) {
        return null;
    }

    return octetsToNumber(addr.split('.'));
}


/*
 * Idempotent conversion from strings (and numbers) to ip6addr objects
 */
function toIPAddr(addr) {
    // If the address passed in is just a series of numbers,
    // convert it to a long that can be parsed by ip6addr
    if (/^[0-9]+$/.test(addr)) {
        addr = Number(addr);
    }

    try {
        return ipaddr.parse(addr);
    } catch (_) {
        return null;
    }
}


/*
 * Old versions of NAPI used to fail to serialize IP addresses, like when
 * writing out the gateway addresses for the "routes" object. This resulted
 * in objects being written to Moray based on ipaddr.js's object, and looked
 * like { "octets": [ 1, 2, 3, 4 ] }.
 *
 * Since ipaddr.js was okay accepting these objects back in this form when
 * fetching from Moray, this went unnoticed until after switching to ip6addr.
 * We now need to fix them up in the places where we failed to serialize them.
 */
function fixupIPAddr(addr) {
    if (typeof (addr) === 'object' && addr.hasOwnProperty('octets')) {
        return toIPAddr(octetsToNumber(addr.octets));
    } else {
        return toIPAddr(addr);
    }
}


function ipAddrPlus(addr, summand) {
    var changed = addr.offset(summand);
    if (changed === null) {
        if (summand > 0) {
            throw new Error('Address overflow!');
        } else {
            throw new Error('Address underflow!');
        }
    }
    return changed;
}


function ipAddrMinus(addr, minuend) {
    return ipAddrPlus(addr, -minuend);
}


var RFC1918Subnets = [
    ipaddr.createCIDR('10.0.0.0', 8),
    ipaddr.createCIDR('172.16.0.0', 12),
    ipaddr.createCIDR('192.168.0.0', 16)
];


/*
 * Returns true if the IP passed in is in any of the RFC1918 private
 * address spaces
 */
function isRFC1918(ip) {
    return RFC1918Subnets.some(function (subnet) {
        return subnet.contains(ip);
    });
}


/*
 * Compares two IP addresses
 */
function compareTo(a, b) {
    return ipaddr.compare(a, b);
}


/*
 * Converts an integer to a dotted IP address
 */
function numberToAddress(num) {
    if (isNaN(num) || num > 4294967295 || num < 0) {
        return null;
    }

    var a = Math.floor(num / 16777216);
    var aR = num - (a * 16777216);
    var b = Math.floor(aR / 65536);
    var bR = aR - (b * 65536);
    var c = Math.floor(bR / 256);
    var d = bR - (c * 256);

    return a + '.' + b + '.' + c + '.' + d;
}


/*
 * Converts CIDR (/xx) bits to netmask
 */
function bitsToNetmask(bits) {
    var n = 0;

    for (var i = 0; i < (32 - bits); i++) {
        n |= 1 << i;
    }
    return numberToAddress(MAX_IPV4 - (n >>> 0));
}


/*
 * Converts netmask to CIDR (/xx) bits
 */
function netmaskToBits(netmask) {
    var num = ~addressToNumber(netmask);
    var b = 0;
    for (b = 0; b < 32; b++) {
        if (num === 0) {
            break;
        }
        num = num >>> 1;
    }
    return 32 - b;
}


module.exports = {
    addressToNumber: addressToNumber,
    aton: addressToNumber,
    bitsToNetmask: bitsToNetmask,
    compareTo: compareTo,
    fixupIPAddr: fixupIPAddr,
    ipAddrMinus: ipAddrMinus,
    ipAddrPlus: ipAddrPlus,
    isRFC1918: isRFC1918,
    netmaskToBits: netmaskToBits,
    numberToAddress: numberToAddress,
    ntoa: numberToAddress,
    toIPAddr: toIPAddr
};
