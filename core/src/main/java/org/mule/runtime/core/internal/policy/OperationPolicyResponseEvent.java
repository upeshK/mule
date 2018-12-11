package org.mule.runtime.core.internal.policy;

import org.mule.runtime.api.el.BindingContext;
import org.mule.runtime.api.event.EventContext;
import org.mule.runtime.api.message.Error;
import org.mule.runtime.api.message.ItemSequenceInfo;
import org.mule.runtime.api.message.Message;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.api.security.Authentication;
import org.mule.runtime.api.security.SecurityContext;
import org.mule.runtime.core.api.context.notification.FlowCallStack;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.message.GroupCorrelation;

import java.util.Map;
import java.util.Optional;


public class OperationPolicyResponseEvent implements CoreEvent {

  public OperationPolicyResponseEvent(CoreEvent operationResponse) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public Map<String, TypedValue<?>> getVariables() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Message getMessage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<Authentication> getAuthentication() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<Error> getError() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCorrelationId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<ItemSequenceInfo> getItemSequenceInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public EventContext getContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BindingContext asBindingContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SecurityContext getSecurityContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<GroupCorrelation> getGroupCorrelation() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FlowCallStack getFlowCallStack() {
    // TODO Auto-generated method stub
    return null;
  }

}
