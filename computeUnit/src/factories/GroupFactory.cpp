#include <factories/GroupFactory.hpp>

#include <operators/GroupOperator.hpp>
#include <Logger.hpp>


GroupFactory::GroupFactory() : AbstractOperatorFactory() {
}

AbstractOperator* GroupFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch( witem.operatorid() ) {
        case OperatorType::OP_GROUPBY: {
            op = new GroupOperator(witem);
        }break;
        default: {
            LOG_ERROR("[GroupFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
        }
    }
    return op;
}
