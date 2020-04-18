const trycatch = async (func, ...args) => {
    const result = {};
    
    try {
        result.result = await func(...args);
    } catch(err) {
        result.err = err;
    }

    return result;
};

module.exports = { trycatch };