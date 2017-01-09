//
//  YARNRMData.swift
//  PerfectHadoop - YARN Resource Manager Data Structures
//
//  Created by Rockford Wei on 2016-01-05.
//	Copyright (C) 2017 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2017 - 2018 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PerfectLib





public struct Token {
  var expirationTime = 0
  var kind = ""
  var maxValidity = 0
  var owner = ""
  var renewer = ""
  var token = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.expirationTime = dictionary["expiration-time"] as? Int ?? 0
    self.kind = dictionary["kind"] as? String ?? ""
    self.maxValidity = dictionary["max-validity"] as? Int ?? 0
    self.owner = dictionary["owner"] as? String ?? ""
    self.renewer = dictionary["renewer"] as? String ?? ""
    self.token = dictionary["token"] as? String ?? ""
  }//init
}//Token

extension String {
  public var asToken: Token? {
    get{
      do{
        return Token(try self.jsonDecode() as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension



public struct Reservations{

  public struct Resource{
    var memory = ""
    var vCores = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.memory = dictionary["memory"] as? String ?? ""
      self.vCores = dictionary["vCores"] as? String ?? ""
    }//init
  }//resourcesUsedType

  public struct ResourceAllocation {
    var resource = Resource()
    var startTime = ""
    var endTime = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.resource = Resource(dictionary["resource"] as? [String:Any] ?? [:] )
      self.startTime = dictionary["startTime"] as? String ?? ""
      self.endTime = dictionary["endTime"] as? String ?? ""
    }//end init
  }//end struct

  public struct ReservationDefinition{

    public struct RerservationRequests {
      public struct ReservationRequest{
        var capability = Resource()
        var duration = ""
        var minConcurrency = ""
        var numContainers = ""
        public init(_ dictionary: [String:Any] = [:]) {
          self.capability = Resource(dictionary["capability"] as? [String:Any] ?? [:])
          self.duration = dictionary["duration"] as? String ?? ""
          self.minConcurrency = dictionary["min-concurrency"] as? String ?? ""
          self.numContainers = dictionary["num-containers"] as? String ?? ""
        }//init
      }//reservation-requestType

      var reservationRequestInterpreter = ""
      var reservationRequest = ReservationRequest()
      public init(_ dictionary: [String:Any] = [:]) {
        self.reservationRequestInterpreter = dictionary["reservation-request-interpreter"] as? String ?? ""
        self.reservationRequest = ReservationRequest(dictionary["reservation-request"] as? [String:Any] ?? [:])
      }//init
    }//end struct

    var arrival = ""
    var deadline = ""
    var reservationName = ""
    var reservationRequests = RerservationRequests()
    public init(_ dictionary: [String:Any] = [:]) {
      self.arrival = dictionary["arrival"] as? String ?? ""
      self.deadline = dictionary["deadline"] as? String ?? ""
      self.reservationName = dictionary["reservation-name"] as? String ?? ""
      self.reservationRequests = RerservationRequests(dictionary["reservation-requests"] as? [String:Any] ?? [:])
    }//init
  }//end struct

  var acceptanceTime = ""
  var user = ""
  var resourceAllocations = [ResourceAllocation]()
  var reservationId = ""
  var reservationDefinition = ReservationDefinition()
  public init (_ dictionary: [String:Any] = [:]) {
    self.acceptanceTime = dictionary["acceptance-time"] as? String ?? ""
    self.user = dictionary["user"] as? String ?? ""
    self.resourceAllocations = (dictionary["resource-allocations"] as? [Any] ?? []).map {ResourceAllocation($0 as? [String:Any] ?? [:]) }
    self.reservationId = dictionary["reservation-id"] as? String ?? ""
    self.reservationDefinition = ReservationDefinition(dictionary["reservation-definition"] as? [String:Any] ?? [:])
  }//end init
}//end struct

extension String {
  public var asReservations: Reservations? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Reservations(dic["reservations"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

